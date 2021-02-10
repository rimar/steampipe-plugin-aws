package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/service/route53"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

//// TABLE DEFINITION

func tableAwsRoute53(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_route53",
		Description: "AWS Route53",
		Get: &plugin.GetConfig{
			KeyColumns:        plugin.SingleColumn("id"),
			ShouldIgnoreError: isNotFoundError([]string{"InvalidParameterValue"}),
			ItemFromKey:       route53FromKey,
			Hydrate:           getAwsRoute53,
		},
		List: &plugin.ListConfig{
			Hydrate: listAwsRoute53s,
		},
		Columns: awsColumns([]*plugin.Column{
			// "Key" Columns
			{
				Name:        "name",
				Description: "The name of the domain. For public hosted zones, this is the name that you have registered with your DNS registrar.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "id",
				Type:        proto.ColumnType_STRING,
				Description: "The ID that Amazon Route 53 assigned to the hosted zone when you created it.",
			},

			// Other Columns
			{
				Name:        "caller_reference",
				Type:        proto.ColumnType_STRING,
				Description: "The value that you specified for CallerReference when you created the hosted zone",
			},
			{
				Name:        "resource_record_setCount",
				Type:        proto.ColumnType_DOUBLE,
				Description: "The number of resource record sets in the hosted zone.",
			},
			{
				Name:        "comment",
				Description: "Any comments that you want to include about the hosted zone",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("HostedZoneConfig.Comment"),
			},
			{
				Name:        "private_zone",
				Description: "If the health check or hosted zone was created by another service, the service that created the resource.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("HostedZoneConfig.PrivateZone"),
			},
			{
				Name:        "linked_service_principal",
				Description: "If the health check or hosted zone was created by another service, the service that created the resource.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("LinkedService.ServicePrincipal"),
			},
			{
				Name:        "description",
				Description: "If the health check or hosted zone was created by another service, the service that created the resource.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("LinkedService.Description"),
			},

			//	Standard columns for all tables
			// {
			// 	Name:        "tags",
			// 	Description: resourceInterfaceDescription("tags"),
			// 	Type:        proto.ColumnType_JSON,
			// 	Hydrate:     getRoute53Tagging,
			// 	Transform:   transform.FromField("TagSet").Transform(route53TagsToTurbotTags),
			// },
			{
				Name:        "akas",
				Description: resourceInterfaceDescription("akas"),
				Type:        proto.ColumnType_JSON,
				Transform:   transform.From(route53NameToAkas),
			},
		}),
	}
}

//// BUILD HYDRATE INPUT

func route53FromKey(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	quals := d.KeyColumnQuals
	zoneName := quals["name"].GetStringValue()
	item := &route53.HostedZone{
		Name: &zoneName,
	}
	return item, nil
}

//// LIST FUNCTION

func listAwsRoute53s(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create Session
	svc, err := Route53Service(ctx, d.ConnectionManager)
	if err != nil {
		return nil, err
	}

	// List call
	err = svc.ListHostedZonesPages(
		&route53.ListHostedZonesInput{},
		func(page *route53.ListHostedZonesOutput, isLast bool) bool {
			for _, route53 := range page.HostedZones {
				d.StreamListItem(ctx, route53)
			}
			return !isLast
		},
	)

	return nil, err
}

//// HYDRATE FUNCTIONS

func getAwsRoute53(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	logger.Trace("getAwsRoute53")
	awsRoute53 := h.Item.(*route53.HostedZone)

	// create service
	svc, err := Route53Service(ctx, d.ConnectionManager)
	if err != nil {
		return nil, err
	}

	params := &route53.GetHostedZoneInput{
		Id: awsRoute53.Id,
	}

	// execute list call
	op, err := svc.GetHostedZone(params)
	if err != nil {
		return nil, err
	}

	if len(op.HostedZone.String()) > 0 {
		return op.HostedZone, nil
	}

	return nil, nil
}

func getRoute53Tagging(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	logger.Trace("getAwsRoute53")
	awsRoute53 := h.Item.(*route53.HostedZone)

	// create service
	svc, err := Route53Service(ctx, d.ConnectionManager)
	if err != nil {
		return nil, err
	}

	params := &route53.ListTagsForResourceInput{
		ResourceId: awsRoute53.Id,
	}

	// List call
	route53Tags, _ := svc.ListTagsForResource(params)

	if err != nil {
		return nil, err
	}

	return route53Tags.ResourceTagSet.Tags, nil
}

//// TRANSFORM FUNCTIONS

func route53NameToAkas(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	plugin.Logger(ctx).Trace("route53NameToAkas")
	hostedZone := d.HydrateItem.(*route53.HostedZone)
	return []string{"arn:aws:route53:::" + *hostedZone.Name}, nil
}

func route53TagsToTurbotTags(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	plugin.Logger(ctx).Trace("route53TagsToTurbotTags")
	tags := d.Value.([]*route53.Tag)

	// Mapping the resource tags inside turbotTags
	var turbotTagsMap map[string]string
	if tags != nil {
		turbotTagsMap = map[string]string{}
		for _, i := range tags {
			turbotTagsMap[*i.Key] = *i.Value
		}
	}
	return turbotTagsMap, nil
}
