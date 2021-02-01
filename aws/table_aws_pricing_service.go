package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/pricing"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

func tableAwsPricingService(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_pricing_service",
		Description: "AWS Pricing - Service List",
		Get: &plugin.GetConfig{
			KeyColumns:        plugin.SingleColumn("service_code"),
			ShouldIgnoreError: isNotFoundError([]string{"NotFoundException"}),
			Hydrate:           getPricingService,
		},
		List: &plugin.ListConfig{
			Hydrate: listPricingServices,
		},
		Columns: awsColumns([]*plugin.Column{
			{
				Name:        "service_code",
				Description: "The code for the AWS service.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "attribute_names",
				Description: "The attributes that are available for this service.",
				Type:        proto.ColumnType_JSON,
			},

			//Standard columns for all tables
			// {
			// 	Name:        "tags",
			// 	Description: resourceInterfaceDescription("tags"),
			// 	Type:        proto.ColumnType_JSON,
			// 	Transform:   transform.FromConstant(nil),
			// },
			{
				Name:        "title",
				Description: resourceInterfaceDescription("title"),
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ServiceCode"),
			},
			// {
			// 	Name:        "akas",
			// 	Description: resourceInterfaceDescription("akas"),
			// 	Type:        proto.ColumnType_JSON,
			// 	Hydrate:     getAwsVpcTurbotData,
			// 	Transform:   transform.FromValue(),
			// },
		}),
	}
}

//// LIST FUNCTION

func listPricingServices(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {

	//defaultRegion := GetDefaultRegion()
	plugin.Logger(ctx).Trace("listPricingServices")

	// Create session
	svc, err := PricingService(ctx, d.ConnectionManager)
	if err != nil {
		return nil, err
	}

	// List call
	err = svc.DescribeServicesPages(
		&pricing.DescribeServicesInput{},
		func(page *pricing.DescribeServicesOutput, isLast bool) bool {
			for _, pricingService := range page.Services {
				d.StreamListItem(ctx, pricingService)
			}
			return !isLast
		},
	)

	return nil, err
}

//// HYDRATE FUNCTIONS

func getPricingService(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	logger.Trace("getPricingService")
	var serviceCode string
	if h.Item != nil {
		ps := h.Item.(*pricing.Service)
		serviceCode = *ps.ServiceCode
	} else {
		serviceCode = d.KeyColumnQuals["service_code"].GetStringValue()
	}

	// Create session
	svc, err := PricingService(ctx, d.ConnectionManager)
	if err != nil {
		return nil, err
	}

	// Build the params
	params := &pricing.DescribeServicesInput{
		ServiceCode: &serviceCode,
	}

	// Get call
	pricingService, err := svc.DescribeServices(params)
	if err != nil {
		logger.Debug("getPricingService", "ERROR", err)
		return nil, err
	}

	if len(pricingService.Services) != 1 {
		return nil, fmt.Errorf("getPricingService failed - Expected 1 result but found %d", len(pricingService.Services))
	}

	return pricingService.Services[0], nil
}
