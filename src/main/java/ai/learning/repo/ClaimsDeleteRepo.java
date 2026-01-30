package com.nontrauma.migration.migrationutil.repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Repository
public class ClaimsDeleteRepo  {

    private static final Logger log = LoggerFactory.getLogger(ClaimsDeleteRepo .class);

    private static final ConsistencyLevel CL = ConsistencyLevel.LOCAL_QUORUM;

    private static final int DUP_DELETE_BATCH_SIZE = 30;

    private static final String DELETE_CLAIM_PARTITION =
            "DELETE FROM claims WHERE payerkey = ? AND memberkey = ?";

    private static final String DELETE_DUP_PARTITION =
            "DELETE FROM claim_duplicate_chk_svcthrudt WHERE payerkey = ? AND memberkey = ?";

    private static final String DELETE_CLAIMS_BY_SERVICE_DATE_RANGE =
            "DELETE FROM claims WHERE payerkey = ? AND memberkey = ? AND servicebegindate >= ? AND servicebegindate < ?";

    private static final String SELECT_CLAIMNUMBERS_BY_SERVICE_DATE_RANGE =
            "SELECT claimnumber FROM claims WHERE payerkey = ? AND memberkey = ? AND servicebegindate >= ? AND servicebegindate < ?";

    private static final String DELETE_DUP_BY_CLAIMNUMBER =
            "DELETE FROM claim_duplicate_chk_svcthrudt WHERE payerkey = ? AND memberkey = ? AND claimnumber = ?";

    private final ClusterReference clusterReference;

    private CqlSession cqlSession;

    private PreparedStatement psDeleteClaimPartition;
    private PreparedStatement psDeleteDupPartition;

    private PreparedStatement psDeleteClaimsByServiceDateRange;
    private PreparedStatement psSelectClaimNumbersByServiceDateRange;
    private PreparedStatement psDeleteDupByClaimNumber;

    @Autowired
    public ClaimsDeleteRepo (final ClusterReference clusterReference) {
        this.clusterReference = clusterReference;
    }

    @PostConstruct
    public void init() {
        try {
            this.cqlSession = Objects.requireNonNull(
                    clusterReference.getSession(),
                    "ClusterReference.getSession() returned null (CqlSession not initialized)"
            );

            this.psDeleteClaimPartition = prepareOrThrow(DELETE_CLAIM_PARTITION);
            this.psDeleteDupPartition = prepareOrThrow(DELETE_DUP_PARTITION);

            this.psDeleteClaimsByServiceDateRange = prepareOrThrow(DELETE_CLAIMS_BY_SERVICE_DATE_RANGE);
            this.psSelectClaimNumbersByServiceDateRange = prepareOrThrow(SELECT_CLAIMNUMBERS_BY_SERVICE_DATE_RANGE);
            this.psDeleteDupByClaimNumber = prepareOrThrow(DELETE_DUP_BY_CLAIMNUMBER);

            log.info("NTClaimDeleteRepo init completed");
        } catch (Exception e) {
            log.error("NTClaimDeleteRepo init failed: {}", e.getMessage(), e);
            throw e;
        }
    }
    private PreparedStatement prepareOrThrow(String cql) {
        PreparedStatement ps = cqlSession.prepare(cql);
        if (ps == null) {
            String errMsg = "Failed to prepare CQL statement: " + cql;
            log.error(errMsg);
            throw new IllegalStateException(errMsg);
        }
        return ps;
    }

    public void deleteClaimsByPayerAndMember(Long payerKey, Long memberKey) {
        // Delete claims partition.
        cqlSession.execute(
                psDeleteClaimPartition.bind(payerKey, memberKey)
                        .setConsistencyLevel(CL)
        );

        // Delete dup partition.
        cqlSession.execute(
                psDeleteDupPartition.bind(payerKey, memberKey)
                        .setConsistencyLevel(CL)
        );

    }

    public void deleteClaimsByPayerMemberAndServiceDateRange(Long payerKey,
                                                            Long memberKey,
                                                            LocalDate fromInclusive,
                                                            LocalDate toExclusive) {

        Set<String> claimNumbers = new HashSet<>();

        BoundStatement selectBs = psSelectClaimNumbersByServiceDateRange
                .bind(payerKey, memberKey, fromInclusive, toExclusive)
                .setConsistencyLevel(CL);

        ResultSet rs = cqlSession.execute(selectBs);
        for (Row row : rs) {
            String claimNumber = row.getString("claimnumber");
            if (claimNumber != null && !claimNumber.isBlank()) {
                claimNumbers.add(claimNumber);
            }
        }

        // Delete claims first.
        cqlSession.execute(
                psDeleteClaimsByServiceDateRange.bind(payerKey, memberKey, fromInclusive, toExclusive)
                        .setConsistencyLevel(CL)
        );

        // Then delete dup rows (unlogged batches to reduce round trips).
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        int added = 0;

        for (String claimNumber : claimNumbers) {
            BoundStatement delDup = psDeleteDupByClaimNumber
                    .bind(payerKey, memberKey, claimNumber)
                    .setConsistencyLevel(CL);

            batch.addStatement(delDup);
            added++;

            if (added >= DUP_DELETE_BATCH_SIZE) {
                cqlSession.execute(batch.build());
                batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
                added = 0;
            }
        }

        if (added > 0) {
            cqlSession.execute(batch.build());
        }

        log.info("Deleted claims range and dup rows by claimnumber; payerkey={}, memberkey={}, claimNumbers={}",
                payerKey, memberKey, claimNumbers.size());
    }
}
