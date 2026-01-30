Absolutely — here is a **clean, polished, documentation‑ready section** you can paste directly into your technical doc or Confluence page.  
This version includes headings, bullet points, callouts, and clear formatting.

***

# 🗂️ **Claim Data Cleanup Flow – Documentation**

## 📌 **1. Overview**

The cleanup logic removes migrated member‑level claim data from Amazon Keyspaces using a **two‑stage deletion strategy**:

1.  **Fast path:** Attempt full **partition delete**
2.  **Fallback path:** If Keyspaces rejects the delete due to range limits, the system performs **adaptive range‑window deletes** using the clustering column `servicebegindate`.

This approach ensures maximum efficiency while still handling Keyspaces limitations gracefully.

***

## 📌 **2. High‑Level Architecture**

```text
Controller → ClaimDataFixService → RangeDeleteNTClaimService → ClaimsDeleteRepository
```

***

## 📌 **3. Detailed Flow**

### **3.1 Controller Layer**

#### `ClaimDataFixController.cleanupByMemberKey(payerKey, files)`

*   Starts an **asynchronous cleanup job** using `TaskExecutor`.
*   Delegates to:
    ```java
    ClaimDataFixService.migratedClaimsDelete(payerKey, files)
    ```

***

### **3.2 Service Layer**

#### `ClaimDataFixService.migratedClaimsDelete(payerKey, files)`

For each S3 file:

1.  Reads the file through  
    `S3Repository.processAndMoveFileFromRawBucket(file, processor, "deleted-members")`
2.  The processor converts each row → `PayerMemberDTO`
3.  Extracts `memberKeys`
4.  Calls:
    ```java
    RangeDeleteNTClaimService.deleteMembers(payerKey, memberKeys)
    ```

***

## 📌 **4. Member Cleanup Logic**

### `RangeDeleteNTClaimService.deleteMembers(payerKey, memberKeys)`

*   Processes each member **in parallel**
*   Each member key is handled via:
    ```java
    deleteWithFallback(payerKey, memberKey)
    ```

***

## 📌 **5. Partition Delete + Fallback Strategy**

### **5.1 Fast Path**

#### `deleteWithFallback(payerKey, memberKey)`

1.  Attempt a **partition-level delete**:
    ```java
    claimsDeleteRepo.deleteClaimsByPayerAndMember(payerKey, memberKey)
    ```
2.  **If successful** → cleanup complete.
3.  **If fails with “range delete requests are limited”** → fallback begins.

***

## 📌 **6. Fallback Delete: Adaptive Range Cleanup**

### **6.1 Yearly Window Loop**

For memberKey:

*   Iterate from **1970‑01‑01 to today + 1 day**, one year at a time.
*   For each yearly window:
    ```java
    deleteRangeAdaptive(payerKey, memberKey, from, to, 12)
    ```

***

### **6.2 Adaptive Delete Execution**

#### `deleteRangeAdaptive(payerKey, memberKey, from, to, monthsPerChunk)`

Runs deletes like:

```sql
DELETE ...
WHERE payerKey=?
  AND memberKey=?
  AND servicebegindate >= ?
  AND servicebegindate < ?
```

If Keyspaces still rejects the delete due to range limits, it automatically reduces the window size:

    12 months → 6 months → 3 months → 1 month

Retries continue until the entire date window is deleted successfully.

***

## 📌 **7. Duplicate Cleanup**

After the entire date range is processed:

```java
claimsDeleteRepo.deleteDupByPayerAndMember(payerKey, memberKey)
```

Ensures any leftover duplicate rows are removed.

***

## 📌 **8. Visual Flow (Per memberKey)**

```text
deleteMembers()
 └── deleteWithFallback()
       ├── Try: deleteClaimsByPayerAndMember()
       │       ├── SUCCESS → DONE
       │       └── FAIL (range limit) → fallback
       └── For each year:
             └── deleteRangeAdaptive(12 months)
                     └── If limit → retry 6 → 3 → 1 month
       └── deleteDupByPayerAndMember()
```

***

If you want, I can also generate:

✨ **Mermaid sequence diagram**  
✨ **Process flowchart (PNG or SVG)**  
✨ **Confluence-compatible formatting**

Just tell me!
