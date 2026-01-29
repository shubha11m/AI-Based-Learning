package ai.learning.service;

// `src/main/java/com/nontrauma/migration/migrationutil/service/MemberClaimsDeleteService.java`
package ai.learning.service;

import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.nontrauma.migration.migrationutil.repository.ClaimsDeleteRepo;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class MemberClaimsDeleteService {
    private static final int THREAD_POOL_SIZE = 30;
    private static final int MAX_RETRIES = 3;

    private final ClaimsDeleteRepo claimsDeleteRepo;
    private final List<Long> rangeDeleteLimitedMemberKeys =
            Collections.synchronizedList(new ArrayList<>());

    @Autowired
    public MemberClaimsDeleteService(ClaimsDeleteRepo claimsDeleteRepo) {
        this.claimsDeleteRepo = claimsDeleteRepo;
    }

    void deleteMembers(Long payerKey, List<Long> memberKeys) {
        memberKeys.stream().parallel()
                .forEach(memberKey -> {
                    deleteWithRetry(payerKey, memberKey);
                });
    }

    private void deleteWithRetry(Long payerKey, Long memberKey) {
        try {
            claimsDeleteRepo.deleteClaimsByPayerAndMember(payerKey, memberKey);
            log.info("Successfully deleted claims for PayerKey: {}, MemberKey: {}", payerKey, memberKey);
        } catch (InvalidQueryException iqe) {
            if (isRangeDeleteLimit(iqe)) {
                rangeDeleteLimitedMemberKeys.add(memberKey);
                log.warn("Range delete limit hit; storing MemberKey {} for later alternate delete logic", memberKey, iqe);
                return;
            }
            log.error("InvalidQueryException deleting claims for PayerKey: {}, MemberKey: {}", payerKey, memberKey, iqe);
        } catch (Exception e) {
            log.error("Failed to delete claims for PayerKey: {}, MemberKey: {}", payerKey, memberKey, e);
        }
    }

    private boolean isRangeDeleteLimit(InvalidQueryException e) {
        String msg = e.getMessage();
        return msg != null && msg.toLowerCase().contains("range delete requests are limited");
    }


    private void sleepBackoff(int attempt) {
        try {
            long backoffTime = (long) Math.pow(2, attempt) * 100L;
            Thread.sleep(backoffTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
