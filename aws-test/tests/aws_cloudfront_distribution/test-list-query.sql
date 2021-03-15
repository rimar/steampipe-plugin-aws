select akas, id, domain_name, is_ipv6_enabled
from aws.aws_cloudfront_distribution
where akas = '["{{ output.resource_aka.value }}"]';
