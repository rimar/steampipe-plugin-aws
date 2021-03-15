package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/service/cloudfront"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

//// TABLE DEFINITION

func tableAwsCloudfrontDistribution(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_cloudfront_distribution",
		Description: "AWS Cloudfront Distribution",
		Get: &plugin.GetConfig{
			KeyColumns:        plugin.SingleColumn("id"),
			ShouldIgnoreError: isNotFoundError([]string{"NoSuchDistribution"}),
			Hydrate:           getCloudfrontDistribution,
		},
		List: &plugin.ListConfig{
			Hydrate: listAwsCloudfrontDistribution,
		},
		Columns: awsRegionalColumns([]*plugin.Column{
			{
				Name:        "id",
				Description: "The identifier for the Distribution.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "enabled",
				Description: "Whether the Distribution is enabled to accept user requests for content.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "e_tag",
				Description: "The current version of the distribution's information.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getCloudfrontDistribution,
			},
			{
				Name:        "status",
				Description: "The current status of the Distribution. When the status is Deployed, the distribution's information is propagated to all CloudFront edge locations.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "last_modified_time",
				Description: "The date and time the Distribution was last modified.",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "domain_name",
				Description: "The domain name that corresponds to the Distribution, for example, d111111abcdef8.cloudfront.net.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "tags_src",
				Description: "A list of tags assigned to the Maintenance Window",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getCloudfrontDistributionTags,
				Transform:   transform.FromField("Tags.Items"),
			},
			{
				Name:        "comment",
				Description: "The comment originally specified when this Distribution was created.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "http_version",
				Description: "Specify the maximum HTTP version that you want viewers to use to communicate with CloudFront. The default value for new web Distributions is http2. Viewers that don't support HTTP/2 will automatically use an earlier version.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "is_ipv6_enabled",
				Description: "Whether CloudFront responds to IPv6 DNS requests with an IPv6 address for your Distribution.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("IsIPV6Enabled"),
			},
			{
				Name:        "active_trusted_key_groups_enabled",
				Description: "This field is true if any of the key groups have public keys that CloudFront can use to verify the signatures of signed URLs and signed cookies. If not, this field is false.",
				Type:        proto.ColumnType_BOOL,
				Hydrate:     getCloudfrontDistribution,
				Transform:   transform.FromField("Distribution.ActiveTrustedKeyGroups.Enabled"),
			},
			{
				Name:        "active_trusted_key_groups_items",
				Description: "A list of key groups, including the identifiers of the public keys in each key group that CloudFront can use to verify the signatures of signed URLs and signed cookies.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getCloudfrontDistribution,
				Transform:   transform.FromField("Distribution.ActiveTrustedKeyGroups.Items"),
			},
			{
				Name:        "active_trusted_key_groups_quantity",
				Description: "The number of key groups in the list.",
				Type:        proto.ColumnType_INT,
				Hydrate:     getCloudfrontDistribution,
				Transform:   transform.FromField("Distribution.ActiveTrustedKeyGroups.Quantity"),
			},
			{
				Name:        "active_trusted_signers_enabled",
				Description: "This field is true if any of the AWS accounts in the list have active CloudFront key pairs that CloudFront can use to verify the signatures of signed URLs and signed cookies. If not, this field is false.",
				Type:        proto.ColumnType_BOOL,
				Hydrate:     getCloudfrontDistribution,
				Transform:   transform.FromField("Distribution.ActiveTrustedSigners.Enabled"),
			},
			{
				Name:        "active_trusted_signers_items",
				Description: "A list of AWS accounts and the identifiers of active CloudFront key pairs in each account that CloudFront can use to verify the signatures of signed URLs and signed cookies.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getCloudfrontDistribution,
				Transform:   transform.FromField("Distribution.ActiveTrustedSigners.Items"),
			},
			{
				Name:        "active_trusted_signers_quantity",
				Description: "The number of AWS accounts in the list.",
				Type:        proto.ColumnType_INT,
				Hydrate:     getCloudfrontDistribution,
				Transform:   transform.FromField("Distribution.ActiveTrustedSigners.Quantity"),
			},
			{
				Name:        "price_class",
				Description: "A complex type that contains information about price class for this streaming Distribution.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "web_acl_id",
				Description: "The Web ACL Id (if any) associated with the distribution..",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("WebACLId"),
			},
			{
				Name:        "aliases_quantity",
				Description: "The number of CNAME aliases, if any, that you want to associate with this Distribution.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Aliases.Quantity"),
			},
			{
				Name:        "aliases_items",
				Description: "A complex type that contains the CNAME aliases, if any, that you want to associate with this distribution.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Aliases.Items"),
			},
			{
				Name:        "cache_behaviors_quantity",
				Description: "The number of cache behaviors for this Distribution.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("CacheBehaviors.Quantity"),
			},
			{
				Name:        "cache_behaviors_items",
				Description: "Optional: A complex type that contains cache behaviors for this Distribution. If Quantity is 0, you can omit Items. ",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("CacheBehaviors.Items"),
			},
			{
				Name:        "origins_quantity",
				Description: "The number of origins for this distribution.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Origins.Quantity"),
			},
			{
				Name:        "origins_items",
				Description: "A list of origins. ",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Origins.Items"),
			},
			{
				Name:        "in_progress_invalidation_batches",
				Description: "A list of origins. ",
				Type:        proto.ColumnType_INT,
				Hydrate:     getCloudfrontDistribution,
				Transform:   transform.FromField("Distribution.InProgressInvalidationBatches"),
			},
			/// Standard columns for all tables
			{
				Name:        "title",
				Description: resourceInterfaceDescription("title"),
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Id"),
			},
			{
				Name:        "tags",
				Description: resourceInterfaceDescription("tags"),
				Type:        proto.ColumnType_JSON,
				Hydrate:     getCloudfrontDistributionTags,
				Transform:   transform.FromField("Tags.Items").Transform(cloudfrontDistributionTagListToTurbotTags),
			},
			{
				Name:        "akas",
				Description: resourceInterfaceDescription("akas"),
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("ARN").Transform(arnToAkas),
			},
		}),
	}
}

//// LIST FUNCTION

func listAwsCloudfrontDistribution(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("listAwsCloudfrontDistribution")

	// Create session
	svc, err := CloudFrontService(ctx, d)
	if err != nil {
		return nil, err
	}

	// List call
	err = svc.ListDistributionsPages(
		&cloudfront.ListDistributionsInput{},
		func(page *cloudfront.ListDistributionsOutput, isLast bool) bool {
			for _, parameter := range page.DistributionList.Items {
				d.StreamListItem(ctx, parameter)
			}
			return !isLast
		},
	)

	return nil, err
}

func getCloudfrontDistribution(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("getCloudfrontDistribution")

	// Create session
	svc, err := CloudFrontService(ctx, d)
	if err != nil {
		return nil, err
	}

	var cloudfrontID string
	if h.Item != nil {
		cloudfrontID = *cloudfrontDistributionID(h.Item)
	} else {
		cloudfrontID = d.KeyColumnQuals["id"].GetStringValue()
	}

	params := &cloudfront.GetDistributionInput{
		Id: &cloudfrontID,
	}

	op, err := svc.GetDistribution(params)
	if err != nil {
		return nil, err
	}

	return op, nil
}

func getCloudfrontDistributionTags(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("getCloudfrontDistributionTags")

	// Create session
	svc, err := CloudFrontService(ctx, d)
	if err != nil {
		return nil, err
	}

	cloudfrontArn := cloudfrontDistributionArn(h.Item)

	// Build the params
	params := &cloudfront.ListTagsForResourceInput{
		Resource: cloudfrontArn,
	}

	// Get call
	op, err := svc.ListTagsForResource(params)
	if err != nil {
		return nil, err
	}

	return op, nil
}

/// TRANSFORM FUNCTIONS

func cloudfrontDistributionTagListToTurbotTags(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	plugin.Logger(ctx).Trace("cloudfrontDistributionTagListToTurbotTags")
	tagList := d.Value.([]*cloudfront.Tag)

	// Mapping the resource tags inside turbotTags
	var turbotTagsMap map[string]string
	if tagList != nil {
		turbotTagsMap = map[string]string{}
		for _, i := range tagList {
			turbotTagsMap[*i.Key] = *i.Value
		}
	} else {
		return nil, nil
	}

	return turbotTagsMap, nil
}

func cloudfrontDistributionID(item interface{}) *string {
	switch item.(type) {
	case *cloudfront.GetDistributionOutput:
		return item.(*cloudfront.GetDistributionOutput).Distribution.Id

	case *cloudfront.DistributionSummary:
		return item.(*cloudfront.DistributionSummary).Id
	}
	return nil
}

func cloudfrontDistributionArn(item interface{}) *string {
	switch item.(type) {
	case *cloudfront.GetDistributionOutput:
		return item.(*cloudfront.GetDistributionOutput).Distribution.ARN

	case *cloudfront.DistributionSummary:
		return item.(*cloudfront.DistributionSummary).ARN
	}
	return nil
}
