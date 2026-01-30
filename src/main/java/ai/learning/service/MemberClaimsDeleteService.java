package com.nontrauma.migration.migrationutil.service;

            import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
            import com.nontrauma.migration.migrationutil.repository.NTClaimDeleteRepo;
            import lombok.extern.slf4j.Slf4j;
            import org.springframework.beans.factory.annotation.Autowired;
            import org.springframework.stereotype.Service;

            import java.time.LocalDate;
            import java.time.ZoneOffset;
            import java.util.List;
            import java.util.Locale;

            /**
             * Service that deletes claim data from Amazon Keyspaces with a fast-path partition delete,
             * and a fallback to adaptive range-window deletes by servicebegindate when range limits are hit.
             *
             * Flow per memberKey:
             * 1) Try partition delete (claims + dup).
             * 2) If range-delete limit is hit, iterate windows [2000-01-01, today+1)
             *    and delete in 12 -> 6 -> 3 -> 1 month chunks.
             *
             * Note: dup cleanup per chunk is expected to be handled inside
             * NTClaimDeleteRepo.deleteClaimsByPayerMemberAndServiceDateRange(...)
             * (select claimnumbers -> delete claims chunk -> delete dup by claimnumber).
             */
            @Slf4j
            @Service
            public class MemberClaimsDeleteService {

                private static final LocalDate DELETE_START = LocalDate.of(2000, 1, 1);

                private final ClaimsDeleteRepo  claimsDeleteRepo;

                @Autowired
                public MemberClaimsDeleteService(ClaimsDeleteRepo  claimsDeleteRepo) {
                    this.claimsDeleteRepo = claimsDeleteRepo;
                }

                void deleteMembers(Long payerKey, List<Long> memberKeys) {
                    memberKeys.forEach(memberKey -> deleteWithFallback(payerKey, memberKey));
                }

                private void deleteWithFallback(Long payerKey, Long memberKey) {
                    try {
                        // Fast path: full partition delete (claims + dup).
                        claimsDeleteRepo.deleteClaimsByPayerAndMember(payerKey, memberKey);
                        log.info("Deleted full partitions for payerKey={}, memberKey={}", payerKey, memberKey);
                        return;
                    } catch (InvalidQueryException iqe) {
                        // Fallback ONLY for the known range-delete-limit error.
                        if (!isRangeDeleteLimit(iqe)) {
                            throw iqe;
                        }
                        log.warn(
                                "Range delete limit hit; falling back to date-window deletes. payerKey={}, memberKey={}, error={}",
                                payerKey, memberKey, safeMessage(iqe)
                        );
                    }

                    LocalDate endExclusive = LocalDate.now(ZoneOffset.UTC).plusDays(1);
                    LocalDate from = DELETE_START;

                    while (from.isBefore(endExclusive)) {
                        LocalDate to = min(from.plusYears(1), endExclusive);
                        deleteRangeAdaptive(payerKey, memberKey, from, to, 12);
                        from = to;
                    }

                    log.info("Deleted partitions via windowed deletes for payerKey={}, memberKey={}", payerKey, memberKey);
                }

                private void deleteRangeAdaptive(Long payerKey, Long memberKey,
                                                 LocalDate fromInclusive, LocalDate toExclusive,
                                                 int monthsPerChunk) {
                    if (!fromInclusive.isBefore(toExclusive)) return;

                    try {
                        int totalMonths = monthsBetween(fromInclusive, toExclusive);

                        if (monthsPerChunk >= totalMonths) {
                            deleteRange(payerKey, memberKey, fromInclusive, toExclusive);
                            return;
                        }

                        LocalDate cursor = fromInclusive;
                        while (cursor.isBefore(toExclusive)) {
                            LocalDate next = min(cursor.plusMonths(monthsPerChunk), toExclusive);
                            deleteRange(payerKey, memberKey, cursor, next);
                            cursor = next;
                        }
                    } catch (InvalidQueryException iqe) {
                        // Shrink chunks ONLY for the known range-delete-limit error.
                        if (!isRangeDeleteLimit(iqe)) {
                            throw iqe;
                        }

                        int nextMonthsPerChunk = nextSmallerChunk(monthsPerChunk);
                        if (nextMonthsPerChunk == monthsPerChunk) {
                            throw iqe;
                        }

                        log.warn(
                                "Claims range delete too large; reducing chunk to {} month(s). payerKey={}, memberKey={}, from={}, to={}, error={}",
                                nextMonthsPerChunk, payerKey, memberKey, fromInclusive, toExclusive, safeMessage(iqe)
                        );

                        deleteRangeAdaptive(payerKey, memberKey, fromInclusive, toExclusive, nextMonthsPerChunk);
                    }
                }

                private void deleteRange(Long payerKey, Long memberKey,
                                         LocalDate fromInclusive, LocalDate toExclusive) {
                    claimsDeleteRepo.deleteClaimsByPayerMemberAndServiceDateRange(payerKey, memberKey, fromInclusive, toExclusive);
                }

                private boolean isRangeDeleteLimit(InvalidQueryException e) {
                    String msg = safeMessage(e);
                    return msg.contains("range delete requests are limited");
                }

                private String safeMessage(Throwable t) {
                    return (t.getMessage() == null) ? "" : t.getMessage().toLowerCase(Locale.ROOT);
                }

                private LocalDate min(LocalDate a, LocalDate b) {
                    return a.isBefore(b) ? a : b;
                }

                private int monthsBetween(LocalDate fromInclusive, LocalDate toExclusive) {
                    return (toExclusive.getYear() - fromInclusive.getYear()) * 12
                            + (toExclusive.getMonthValue() - fromInclusive.getMonthValue());
                }

                private int nextSmallerChunk(int monthsPerChunk) {
                    if (monthsPerChunk > 6) return 6;
                    if (monthsPerChunk > 3) return 3;
                    if (monthsPerChunk > 1) return 1;
                    return 1;
                }
            }
