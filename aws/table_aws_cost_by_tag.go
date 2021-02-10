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

func tableAwsCostByTag(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_cost_by_tag",
		Description: "AWS Cost Explorer - Cost by Tag",
		List: &plugin.ListConfig{
			KeyColumns: plugin.AllColumns([]string{"granularity", "tag_key"}),
			Hydrate:    listCostByTag,
		},
		Columns: awsColumns(
			costExplorerColumns([]*plugin.Column{

				{
					Name:        "tag_key",
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
			}),
		),
	}
}

//// LIST FUNCTION

func listCostByTag(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	params := buildCostByTagInput(d.KeyColumnQuals)
	return streamCostAndUsage(ctx, d, params)
}

func buildCostByTagInput(keyQuals map[string]*proto.QualValue) *costexplorer.GetCostAndUsageInput {
	granularity := strings.ToUpper(keyQuals["granularity"].GetStringValue())
	tagKey := keyQuals["tag_key"].GetStringValue()

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
				Type: aws.String("TAG"),
				Key:  aws.String(tagKey),
			},
		},
	}

	return params
}
