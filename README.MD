# Exercise 1

## HP Functions

### Δομή
Η δομή HP_info κρατάει μεταδεδομένα που σχετίζονται με το αρχείο σωρού που αναπαριστά το HP. Η δομή αποτελείται από τα πεδία:
1) fileDesc: ο αριθμός του ανοικτού αρχείου σωρού
2) headerPosition: η θέση του header στο αρχείο σωρού
3) recordsPerBlock: ο αριθμός των εγγραφών που χωράει ένα block
4) isHeapFile/isHash: ένα flag που δείχνει αν το αρχείο σωρού είναι ένα αρχείο σωρού ή ένα αρχείο hash
Η δομή HP_BlockInfo κρατάει μεταδεδομένα που σχετίζονται με ένα block του αρχείου σωρού. Η δομή αποτελείται από τα πεδία:
1) currentRecords: ο τρέχων αριθμός των εγγραφών που υπάρχουν στο block
2) recordsCount: η χωρητικότητα του block σε εγγραφές η οποία είναι ( sizeof(char) * BF_BLOCK_SIZE - sizeof(HP_block_info) ) / sizeof(Record)
3) nextBlock: η θέση του επόμενου block στο αρχείο σωρού