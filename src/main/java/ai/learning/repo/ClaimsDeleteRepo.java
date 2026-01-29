package ai.learning.repo;


package com.nontrauma.migration.migrationutil.repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class ClaimsDeleteRepo {

    private static final Logger log = LoggerFactory.getLogger(ClaimsDeleteRepo.class);

    private static final String DELETE_CLAIM = "delete from claims where PayerKey = ? and memberKey =?";
    private static final String DELETE_DUP_QUERY = "delete from claim_duplicate_chk_svcthrudt where payerkey = ? and memberkey = ?";

    private final ClusterReference clusterReference;

    private CqlSession cqlSession;
    private PreparedStatement preparedStatementForDeleteClaims;
    private PreparedStatement preparedStatementForDeleteDupCheck;

    @Autowired
    public ClaimsDeleteRepo(final ClusterReference clusterReference) {
        this.clusterReference = clusterReference;
    }

    @PostConstruct
    public void init() {
        this.cqlSession = clusterReference.getSession();
        this.preparedStatementForDeleteClaims = this.cqlSession.prepare(DELETE_CLAIM);
        this.preparedStatementForDeleteDupCheck = this.cqlSession.prepare(DELETE_DUP_QUERY);
    }

    public void deleteClaimsByPayerAndMember(Long payerKey, Long memberKey) {
        try {
            this.cqlSession.execute(
                    preparedStatementForDeleteClaims.bind(payerKey, memberKey).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            );
            this.cqlSession.execute(preparedStatementForDeleteDupCheck.bind(payerKey, memberKey).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            );
        } catch (Exception e) {
            log.error("Error deleting claims for payerKey={}, memberKey={}", payerKey, memberKey, e);
            throw e;
        }
    }
}
