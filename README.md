Business Rules for PSA, Person, and Institution Data Pipeline
1. Data Ingestion Rules
The pipeline reads data from three sources:

PSA Dataset: Contains records related to treating facility and person identifiers.

Person Dataset: Contains ICN and status for individuals.

Institution Dataset: Maps institution SIDs to their respective codes.

Data is read from Parquet files to ensure efficient processing.

2. Filtering Rules
PSA Data Filtering
Only records where MVITreatingFacilityInstitutionSID is 5667, 6061, or 6722 are retained.

Records must meet one of the following conditions:

ActiveMergedIdentifier is 'Active'

ActiveMergedIdentifier is NULL

Records missing MVIPersonICN are excluded to maintain valid person identifiers.

Person Data Filtering
Only the latest available record per MVIPersonICN is considered.

3. Deduplication Rules
PSA Data Deduplication
If multiple PSA records exist for the same MVIPersonICN and MVITreatingFacilityInstitutionSID, only the latest record is retained, based on CorrelationModifiedDateTime.

Person Data Deduplication
If multiple person records exist for the same MVIPersonICN, only the latest record is retained, based on calc_IngestionTimestamp.

4. Joining Rules
PSA Data to Institution Data
PSA records are joined with the Institution dataset using MVITreatingFacilityInstitutionSID to fetch the corresponding InstitutionCode.

Final Data Enrichment
The deduplicated PSA records are joined with the latest Person ICN records to include ICNStatus.

5. Duplicate Conflict Resolution Rules
Institution and Person ID Conflicts
If multiple Treating Facility Person Identifiers exist for the same MVIPersonICN within an InstitutionCode, the record is flagged as a duplicate and removed.

If multiple MVIPersonICN exist for the same Treating Facility Person Identifier within an InstitutionCode, the record is flagged as a duplicate and removed.

6. Data Pivoting and Final Output Rules
The pipeline pivots the data to create separate columns for each Institution Code:

200CORP → participant_id

200DOD → edipi

200VETS → va_profile_id

If a MVIPersonICN does not have a Treating Facility Person Identifier for an institution, the corresponding column value is NULL.

The final output dataset contains:

MVIPersonICN

participant_id (200CORP)

edipi (200DOD)

va_profile_id (200VETS)

icn_status

7. Expected Behavior for a First-Time User or a New Pipeline
If a new user runs the pipeline with fresh data:

Only the latest and valid PSA and Person records will be processed.

Any conflicting duplicate records will be automatically removed.

The final dataset will contain a single row per MVIPersonICN, structured with three separate columns for 200CORP, 200DOD, and 200VETS.

If a new pipeline is created from scratch:

The business rules must be followed to ensure consistency in filtering, deduplication, and pivoting.

The window functions for ranking must be applied to maintain only the latest records.

The joins must be structured to integrate PSA, Institution, and Person data correctly.

The pivot transformation must be applied to generate the final format.

Key Takeaways
The pipeline ensures only the most recent and valid records are processed.

Duplicate conflicts are detected and removed before final processing.

The final output provides a structured, deduplicated, and enriched dataset for downstream analysis.
