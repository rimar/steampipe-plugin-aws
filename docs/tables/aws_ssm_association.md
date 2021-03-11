# Table: aws_ssm_association

AWS Systems Manager association resource creates a State Manager association for your managed instances. The association applies the configuration also specifies actions to take when applying the configuration. 

## Examples

### Basic info

```sql
select
  association_id,
  association_name,
  association_version,
  last_execution_date,
  document_name,
  region
from
  aws_ssm_association;
```


### List associations with a failed status
 
```sql
select
  association_id,
  overview ->> 'AssociationStatusAggregatedCount' as association_status_aggregated_count,
  overview ->> 'DetailedStatus' as detailed_status,
  overview ->> 'Status' as status
from
  aws_ssm_association
where
  overview ->> 'Status' = 'Failed';
```


### List of instances targeted by associations

```sql
select
  association.association_id as association_id,
  target ->> 'Key' as target_key,
  target ->> 'Values' as target_value,
  instances
from
  aws_ssm_association as association,
  jsonb_array_elements(targets) as target,
  jsonb_array_elements_text(target -> 'Values') as instances
where
  target ->> 'Key' = 'InstanceIds';
```


### List associations with critical compliance severity

```sql
select
  association_id,
  association_name,
  targets,
  document_name
from
  aws_ssm_association
where
  compliance_severity = 'CRITICAL';
```
