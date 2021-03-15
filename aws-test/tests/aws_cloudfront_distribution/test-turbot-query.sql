select title, akas, tags, region, account_id
from aws.aws_cloudfront_distribution
where id = '{{ output.resource_id.value }}';