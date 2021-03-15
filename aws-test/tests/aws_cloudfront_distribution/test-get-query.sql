select title, akas, tags, status, domain_name
from aws.aws_cloudfront_distribution
where id = '{{ output.resource_id.value }}';