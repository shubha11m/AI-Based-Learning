package ai.learning.controller;

import ai.learning.service.ClaimDeleteService;

import java.util.List;

public class cpController {
    @@Autowied
    private ClaimDeleteService claimDeleteService70;
    @GetMapping()
    public void migratedClaimsDelete(Long payerkey, List<String> files) {
        claimDeleteService70.migratedClaimsDelete(payerkey,files);
    }
}
