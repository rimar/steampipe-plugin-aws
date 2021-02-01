package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/pricing"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

func tableAwsPricingProduct(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_pricing_service",
		Description: "AWS Pricing - Service List",
		// Get: &plugin.GetConfig{
		// 	KeyColumns:        plugin.AllColumns([]string{"service_code"}),
		// 	ShouldIgnoreError: isNotFoundError([]string{"NotFoundException"}),
		// 	Hydrate:           getPricingProduct,
		// },
		List: &plugin.ListConfig{
			KeyColumns: plugin.AllColumns([]string{"service_code", "instance_type"}),
			Hydrate:    listPricingProducts,
		},
		Columns: awsColumns([]*plugin.Column{

			// Top-level fields
			{
				Name:        "service_code",
				Description: "The code for the AWS service.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("serviceCode"),
				//Transform: transform.FromConstant("AmazonEKS"),
			},
			{
				Name:        "publicationDate",
				Description: "",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("publicationDate").NullIfZero(),
			},
			{
				Name:        "version",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("version"),
			},

			// fields in all `products`
			{
				Name:        "sku",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.sku"),
			},
			{
				Name:        "product_family",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.productFamily"),
			},

			// // this is listed as an attribute, but never appears in the
			// // attributes list??  onlt to filter? OnDemand, Reserved
			// {
			// 	Name:        "term_type",
			// 	Description: "",
			// 	Type:        proto.ColumnType_STRING,
			// 	Transform:   transform.FromField("product.attributes.TermType"),
			// },

			// product.attributes for both RDS instances ane EC2 instances
			{
				Name:        "vcpu",
				Description: "",
				Type:        proto.ColumnType_INT,
				Transform:   transform.FromField("product.attributes.vcpu"),
			},
			{
				Name:        "memory",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.memory"),
			},

			{
				Name:        "storage",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.storage"),
			},

			{
				Name:        "location",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.location"),
			},
			{
				Name:        "operation",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.operation"),
			},
			{
				Name:        "usage_type",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.usagetype"),
			},
			{
				Name:        "clock_speed",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.clockSpeed"),
			},
			{
				Name:        "service_name",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.servicename"),
			},
			{
				Name:        "instance_type",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.instanceType"),
			},
			{
				Name:        "license_model",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.licenseModel"),
			},
			{
				Name:        "location_type",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.locationType"),
			},
			{
				Name:        "instance_family",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.instanceFamily"),
			},
			{
				Name:        "current_generation",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.currentGeneration"),
			},
			{
				Name:        "physical_processor",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.physicalProcessor"),
			},
			{
				Name:        "processor_features",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.processorFeatures"),
			},
			{
				Name:        "network_performance",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.networkPerformance"),
			},

			{
				Name:        "processor_architecture",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.processorArchitecture"),
			},

			{
				Name:        "normalization_size_factor",
				Description: "",
				Type:        proto.ColumnType_STRING, // usually a number, but sometimes 'NA'.  possibly transform NUllIFEqual ??
				Transform:   transform.FromField("product.attributes.normalizationSizeFactor"),
			},

			// EC2-Specific product.attributes
			{
				Name:        "ecu",
				Description: "",
				Type:        proto.ColumnType_STRING, // usually a number, sometimes 'Variable'
				Transform:   transform.FromField("product.attributes.ecu"),
			},
			{
				Name:        "tenancy",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.tenancy"),
			},
			{
				Name:        "capacity_status",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.capacitystatus"),
			},
			{
				Name:        "pre_installed_sw",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.preInstalledSw"),
			},
			{
				Name:        "operating_system",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.operatingSystem"),
			},
			// TO DO: should the yes/no cols be bools?
			{
				Name:        "intel_avx_available",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.intelAvxAvailable"),
			},

			{
				Name:        "intel_avx2_available",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.intelAvx2Available"),
			},
			{
				Name:        "intel_turbo_available",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.intelTurboAvailable"),
			},
			{
				Name:        "dedicated_ebs_throughput",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.dedicatedEbsThroughput"),
			},
			{
				Name:        "enhanced_networking_supported",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.enhancedNetworkingSupported"),
			},

			// ec2....
			//!  "ecu": "10",
			//x  "vcpu": "2",
			//x  "memory": "8 GiB",
			//x  "storage": "EBS only",
			//!  "tenancy": "NA",
			//x  "location": "US East (N. Virginia)",
			//x  "operation": "Hourly",
			//x  "usagetype": "EBSOptimized:m5.large",
			//x  "clockSpeed": "3.1 GHz",
			//x  "servicecode": "AmazonEC2",
			//x  "servicename": "Amazon Elastic Compute Cloud",
			//x  "instanceType": "m5.large",
			//x  "licenseModel": "NA",
			//x  "locationType": "AWS Region",
			//!  "capacitystatus": "NA",
			//x  "instanceFamily": "General purpose",
			//!  "preInstalledSw": "NA",
			//!  "operatingSystem": "NA",
			//x  "currentGeneration": "Yes",
			//!  "intelAvxAvailable": "Yes",
			//x  "physicalProcessor": "Intel Xeon Platinum 8175 (Skylake)",
			//x  "processorFeatures": "Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo",
			//!  "intelAvx2Available": "Yes",
			//x  "networkPerformance": "Up to 10 Gigabit",
			//!  "intelTurboAvailable": "Yes",
			//x  "processorArchitecture": "64-bit",
			//!  "dedicatedEbsThroughput": "Up to 2120 Mbps",
			//x  "normalizationSizeFactor": "4",
			//!  "enhancedNetworkingSupported": "Yes"

			// RDS...
			//x "vcpu": "1",
			//x "memory": "2 GiB",
			//x "storage": "EBS Only",
			//x "location": "US East (N. Virginia)",
			//x "operation": "CreateDBInstance:0004",
			//x "usagetype": "Multi-AZUsage:db.t2.small",
			//x "clockSpeed": "Up to 3.3 GHz",
			//! "engineCode": "4",
			//x "servicecode": "AmazonRDS",
			//x "servicename": "Amazon Relational Database Service",
			//x "instanceType": "db.t2.small",
			//x "licenseModel": "Bring your own license",
			//x "locationType": "AWS Region",
			//! "databaseEngine": "Oracle",
			//x "instanceFamily": "General purpose",
			//! "databaseEdition": "Standard",
			//! "deploymentOption": "Multi-AZ",
			//x "currentGeneration": "No",
			//x "physicalProcessor": "Intel Xeon Family",
			//x "processorFeatures": "Intel AVX; Intel Turbo",
			//! "instanceTypeFamily": "T2",
			//x "networkPerformance": "Low to Moderate",
			//x "processorArchitecture": "32-bit or 64-bit",
			//x "normalizationSizeFactor": "2"

			// RDS-Specific product.attributes
			{
				Name:        "engine_code",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.engineCode"),
			},
			{
				Name:        "database_engine",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.databaseEngine"),
			},
			{
				Name:        "database_edition",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.databaseEdition"),
			},
			{
				Name:        "deployment_option",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.deploymentOption"),
			},
			{
				Name:        "instance_type_family",
				Description: "",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("product.attributes.instanceTypeFamily"),
			},

			//raw .....
			{
				Name:        "attributes",
				Description: "",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("product.attributes"),
			},
			{
				Name:        "terms",
				Description: "",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("terms"),
			},
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
	}
}

//// LIST FUNCTION

func listPricingProducts(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	logger.Trace("listPricingProducts")

	serviceCode := d.KeyColumnQuals["service_code"].GetStringValue()
	instanceType := d.KeyColumnQuals["instance_type"].GetStringValue()
	//location := d.KeyColumnQuals["location"].GetStringValue()
	//location := "US East (N. Virginia)" //US West (N. California)
	// Create session
	svc, err := PricingService(ctx, d.ConnectionManager)
	if err != nil {
		return nil, err
	}

	params := &pricing.GetProductsInput{
		ServiceCode: &serviceCode,
		Filters: []*pricing.Filter{
			// {
			// 	Field: aws.String("termType"),
			// 	Type:  aws.String("TERM_MATCH"),
			// 	Value: aws.String("OnDemand"),
			// },
			// {
			// 	Field: aws.String("location"),
			// 	Type:  aws.String("TERM_MATCH"),
			// 	Value: aws.String(location),
			// },
			{
				Field: aws.String("instanceType"),
				Type:  aws.String("TERM_MATCH"),
				Value: aws.String(instanceType),
			},
		},
	}
	// List call
	err = svc.GetProductsPages(
		params,
		func(page *pricing.GetProductsOutput, isLast bool) bool {
			for _, product := range page.PriceList {
				// item, err := awsJSONValueToString(product)
				// if err != nil {
				// 	logger.Error("Error converting aws.JSONValue", "err", err)
				// }
				// logger.Warn("listPricingProducts", "item", item)

				// d.StreamListItem(ctx, item)
				d.StreamListItem(ctx, product)
			}
			return !isLast
		},
	)

	return nil, err
}

// func awsJSONValueToString(v aws.JSONValue) (string, error) {
// 	bytes, err := jsonutil.BuildJSON(v)
// 	if err != nil {
// 		return "", err
// 	}
// 	return strconv.Unquote(string(bytes))
// }

//// HYDRATE FUNCTIONS

// func getPricingProduct(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
// 	logger := plugin.Logger(ctx)
// 	logger.Warn("getPricingProduct")
// 	var serviceCode string
// 	if h.Item != nil {
// 		ps := h.Item.(*pricing.Service)
// 		serviceCode = *ps.ServiceCode
// 	} else {
// 		serviceCode = d.KeyColumnQuals["service_code"].GetStringValue()
// 	}

// 	logger.Warn("getPricingProduct", "serviceCode", serviceCode)

// 	// Create session
// 	svc, err := PricingService(ctx, d.ConnectionManager)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Build the params
// 	params := &pricing.DescribeServicesInput{
// 		ServiceCode: &serviceCode,
// 	}
// 	logger.Warn("getPricingProduct", "params", params)
// 	logger.Warn("getPricingProduct", "params.ServiceCode", params.ServiceCode)

// 	// Get call
// 	pricingService, err := svc.DescribeServices(params)
// 	if err != nil {
// 		logger.Debug("getPricingProduct", "ERROR", err)
// 		return nil, err
// 	}

// 	if len(pricingService.Services) != 1 {
// 		return nil, fmt.Errorf("getPricingProduct failed - Expected 1 result but found %d", len(pricingService.Services))
// 	}
// 	logger.Warn("getPricingProduct", "pricingService", pricingService)
// 	logger.Warn("getPricingProduct", "pricingService.Services", pricingService.Services)

// 	return pricingService.Services[0], nil
// }
