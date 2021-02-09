package aws

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/costexplorer"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
)

func tableAwsCostForecast(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_cost_forecast",
		Description: "AWS Cost Explorer - Cost Forecast",
		List: &plugin.ListConfig{
			KeyColumns: plugin.SingleColumn("granularity"),
			Hydrate:    listCostForecast,
		},
		Columns: awsColumns([]*plugin.Column{

			// {
			// 	Name:        "linked_account_id",
			// 	Description: "",
			// 	Type:        proto.ColumnType_STRING,
			// 	Transform:   transform.FromField("Dimension1"),
			// },

			// Quals columns - to filter the lookups
			{
				Name:        "granularity",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Hydrate:     hydrateCostAndUsageQuals,
			},

			{
				Name:        "period_start",
				Description: "Start timestamp for this cost metric",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "period_end",
				Description: "End timestamp for this cost metric",
				Type:        proto.ColumnType_TIMESTAMP,
			},

			{
				Name:        "mean_value",
				Description: "Average forecasted value",
				Type:        proto.ColumnType_DOUBLE,
			},

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
		},
		),
	}
}

//// LIST FUNCTION

func listCostForecast(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	//params := buildCostForecastInput(d.KeyColumnQuals)
	//return streamCostAndUsage(ctx, d, params)

	logger := plugin.Logger(ctx)
	logger.Trace("listCostForecast")

	// Create session
	svc, err := CostExplorerService(ctx, d.ConnectionManager)
	if err != nil {
		return nil, err
	}

	params := &costexplorer.GetCostForecastInput{
		TimePeriod: &costexplorer.DateInterval{
			Start: aws.String(startTime),
			End:   aws.String(endTime),
		},
		Granularity: aws.String(granularity),
		Metric:      aws.String(metric),
	}

	// List call
	morePages := true
	for morePages {
		output, err := svc.GetCostForecast(params)
		if err != nil {
			logger.Error("listCostForecast", "err", err)
			return nil, err
		}

		// stream the results...
		for _, row := range buildMetricRows(ctx, output, d.KeyColumnQuals) {
			d.StreamListItem(ctx, row)
		}

		// get more pages if there are any...
		if output.NextPageToken == nil {
			morePages = false
			break
		}
		params.SetNextPageToken(*output.NextPageToken)
	}

	return nil, nil
}

func buildCostForecastInput(keyQuals map[string]*proto.QualValue) *costexplorer.GetCostForecastInput {
	granularity := strings.ToUpper(keyQuals["granularity"].GetStringValue())
	//metric := strings.ToUpper(keyQuals["metric"].GetStringValue())
	metric := "UNBLENDED_COST"
	timeFormat := "2006-01-02"
	if granularity == "HOURLY" {
		timeFormat = "2006-01-02T15:04:05Z"
	}
	endTime := time.Now().Format(timeFormat)
	startTime := getStartDateForGranularity(granularity).Format(timeFormat)

	params := &costexplorer.GetCostForecastInput{
		TimePeriod: &costexplorer.DateInterval{
			Start: aws.String(startTime),
			End:   aws.String(endTime),
		},
		Granularity: aws.String(granularity),
		Metric:      aws.String(metric),
	}

	return params
}



func getForecastEndDateForGranularity(granularity string) time.Time {
	switch granularity {
	case "MONTHLY":
		// 1 year
		return time.Now().AddDate(-1, 0, 0)
	case "DAILY":
		// 3 months
		return time.Now().AddDate(0, 0, -13)
	}
	return time.Now().AddDate(0, 0, -13)
}


