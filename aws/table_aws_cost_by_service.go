package aws

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/costexplorer"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

func tableAwsCostByService(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_cost_by_service",
		Description: "AWS Cost Explorer - Cost by Service",
		List: &plugin.ListConfig{
			KeyColumns: plugin.SingleColumn("granularity"),
			Hydrate:    listCostByService,
		},
		Columns: awsColumns(
			costExplorerColumns([]*plugin.Column{

				{
					Name:        "service",
					Description: "",
					Type:        proto.ColumnType_STRING,
					Transform:   transform.FromField("Dimension1"),
				},

				// Quals columns - to filter the lookups
				{
					Name:        "granularity",
					Description: "",
					Type:        proto.ColumnType_STRING,
					Hydrate:     hydrateCostAndUsageQuals,
				},
				// {
				// 	Name:        "raw_quals",
				// 	Description: "",
				// 	Type:        proto.ColumnType_STRING,
				// 	Hydrate:     hydrateKeyQuals,
				// 	Transform:   transform.FromValue(),
				// },
				// {
				// 	Name:        "raw",
				// 	Description: "raw data",
				// 	Type:        proto.ColumnType_JSON,
				// 	Transform:   transform.FromValue(),
				// },

				//Standard columns for all tables
				// {
				// 	Name:        "tags",
				// 	Description: resourceInterfaceDescription("tags"),
				// 	Type:        proto.ColumnType_JSON,
				// 	Transform:   transform.FromConstant(nil),
				// },
				// {
				// 	Name:        "title",
				// 	Description: resourceInterfaceDescription("title"),
				// 	Type:        proto.ColumnType_STRING,
				// 	Transform:   transform.FromField("ServiceCode"),
				// },
				// {
				// 	Name:        "akas",
				// 	Description: resourceInterfaceDescription("akas"),
				// 	Type:        proto.ColumnType_JSON,
				// 	Hydrate:     getAwsVpcTurbotData,
				// 	Transform:   transform.FromValue(),
				// },
			}),
		),
	}
}

//// LIST FUNCTION

func listCostByService(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	params := buildCostByServiceInput(d.KeyColumnQuals)
	return streamCostAndUsage(ctx, d, params)
}

func buildCostByServiceInput(keyQuals map[string]*proto.QualValue) *costexplorer.GetCostAndUsageInput {
	granularity := strings.ToUpper(keyQuals["granularity"].GetStringValue())
	timeFormat := "2006-01-02"
	if granularity == "HOURLY" {
		timeFormat = "2006-01-02T15:04:05Z"
	}
	endTime := time.Now().Format(timeFormat)
	startTime := getStartDateForGranularity(granularity).Format(timeFormat)

	params := &costexplorer.GetCostAndUsageInput{
		TimePeriod: &costexplorer.DateInterval{
			Start: aws.String(startTime),
			End:   aws.String(endTime),
		},
		Granularity: aws.String(granularity),
		Metrics:     aws.StringSlice(AllCostMetrics()),
		GroupBy: []*costexplorer.GroupDefinition{
			{
				Type: aws.String("DIMENSION"),
				Key:  aws.String("SERVICE"),
			},
		},
	}

	return params
}
