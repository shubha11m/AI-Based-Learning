package ai.learning.service;


public class ClaimDeleteService {

    @Autowied
    private MemberClaimsDeleteService memberClaimsDeleteService;
    @Async
    public void migratedClaimsDelete(Long payerkey, List<String> files) {
        log.info("Invoked migrateClaims for payerkey {} ", payerkey);

        Consumer<byte[]> processor = t -> {
            log.info("Processor is invoked..");
            final List<PayerMemberDTO> claims = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new ByteArrayInputStream(t), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    PayerMemberDTO payerMemberDTO = objectMapper.readValue(line, PayerMemberDTO.class);
                    claims.add(payerMemberDTO);
                }
                if (!claims.isEmpty())
                    executeDelete(payerkey, claims);
            } catch (Exception e) {
                log.error("Error while migrating claims for {}", payerkey, e);
            }
        };
        log.info("Before calling s3repository for {}", payerkey);

        files.forEach(file -> {
            this.s3Repository.processAndMoveFileFromRawBucket(file, processor, "deleted-members");
        });
    }

    private void executeDelete(final Long payerKey, final List<PayerMemberDTO> claims) {
        log.info("Before Execute delete invoked");
        final List<Long> memberKeys = claims.stream()
                .map(PayerMemberDTO::getMemberKey)
                .toList();
        this.memberClaimsDeleteService.deleteMembers(payerKey, memberKeys);
        log.info("After Execute delete invoked");
    }
    )
}
