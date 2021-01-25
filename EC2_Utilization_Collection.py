#!/usr/bin/python


import boto3
import os, sys, csv, datetime, io, re, itertools, sys
import numpy as np
from datetime import datetime, timedelta
import botocore.exceptions


# encoding=utf8

#---------------------------------------------------------------------------------------------------------
# USAGE 	   	        : Collect EC2 Utilization
# How to Run Example    : python Collect_EC2_Utilization.py "us-east-1"
#---------------------------------------------------------------------------------------------------------

#---------------------------------------------------------------------------------------------------------
#Global variables
#---------------------------------------------------------------------------------------------------------

daysTocheck_1 = 10;
daysTocheck_2 = 5;
Metric_Period = 300; #300s, 5 mins per datapoint
Metric_Period_Total = 600;
Percentile_To_Check = 99; 
MetricNamespace = 'AWS/EC2';

if len(sys.argv) == 2:
    region = sys.argv[1];
else:
    print ("--------------------------------------------------");
    print ("Please provide a region ");
    print (__file__+" [region]");
    print ("eg:- >"+__file__+" us-west-1");
    print ("--------------------------------------------------");

    sys.exit(1);


timestamp = '{:%Y-%m-%d-%H:%M:%S}'.format(datetime.utcnow());
Result_fileout = region+"_ec2_utilization_"+timestamp+".csv";

now = datetime.utcnow();
start_time = now - timedelta(days=daysTocheck_1);
end_time = now - timedelta(minutes=5);

period_1_start_time = now - timedelta(days=daysTocheck_1);
period_1_end_time = now - timedelta(days=daysTocheck_2) - timedelta(minutes=5);

period_2_start_time = now - timedelta(days=daysTocheck_2);
period_2_end_time = now - timedelta(minutes=5);

print ("\n");
print ("\n");
print ("working on region: {0}".format(region));
print ("");

ec2client = boto3.client('ec2',region_name=region);
cwatchclient = boto3.client('cloudwatch',region_name=region);


def CollectMetric(Resource_ID,MetricNamespace,MetricName,StatisticsType,ServiceDimensions,Metric_Period,Metric_Start_Time,Metric_End_time):

    MetricData = cwatchclient.get_metric_statistics(
    Namespace=MetricNamespace,
    MetricName=MetricName,
    Dimensions=[{'Name': ServiceDimensions, 'Value': Resource_ID}],
    StartTime=Metric_Start_Time,
    EndTime=Metric_End_time,
    Period=Metric_Period, #every 5min
    Statistics=[StatisticsType]);
    
    return MetricData;



def GetEC2Metric(InstanceID):

    CPUUtilization_arr =[];
    DiskReadOps_arr =[];
    DiskWriteOps_arr =[];
    DiskReadBytes_arr =[];
    DiskWriteBytes_arr =[];
    NetworkIn_arr =[];
    NetworkOut_arr =[];
    csv_arr =[];
    psa_product_id = "";


    try:

        EC2_Info = ec2client.describe_instances(
            InstanceIds=[
                InstanceID,
            ]
        )

        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['InstanceId']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['InstanceType']);

        try:
            csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['InstanceLifecycle']);
        except:
            csv_arr.append("Ondemand");

        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['ImageId']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['LaunchTime']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['Monitoring']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['Placement']['AvailabilityZone']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['Placement']['Tenancy']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['State']['Name']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['SubnetId']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['VpcId']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['Architecture']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['BlockDeviceMappings'][0]['Ebs']['DeleteOnTermination']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['EbsOptimized']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['EnaSupport']);
        csv_arr.append(EC2_Info['Reservations'][0]['Instances'][0]['Hypervisor']);
        for tag in EC2_Info['Reservations'][0]['Instances'][0]['Tags']:
            if tag['Key'] == 'psa_product_id':
                csv_arr.append(tag['Value']);
                psa_product_id = tag['Value'];

        if psa_product_id == "":
            csv_arr.append("no psa id");


        csv_arr.append(start_time);
        csv_arr.append(end_time);

        print ("Collecting metric for " + EC2_Info['Reservations'][0]['Instances'][0]['InstanceId']);

        try:

            CPUUtilization = CollectMetric(InstanceID,MetricNamespace,'CPUUtilization','Maximum','InstanceId',Metric_Period_Total,start_time,end_time);
            CPUUtilization_1 = CollectMetric(InstanceID,MetricNamespace,'CPUUtilization','Maximum','InstanceId',Metric_Period,period_1_start_time,period_1_end_time);
            CPUUtilization_2 = CollectMetric(InstanceID,MetricNamespace,'CPUUtilization','Maximum','InstanceId',Metric_Period,period_2_start_time,period_2_end_time);


            if CPUUtilization['Datapoints']:
                for datapoints in CPUUtilization_1['Datapoints']:

                    CPUUtilization_arr.append(datapoints['Maximum']);

                for datapoints in CPUUtilization_2['Datapoints']:

                    CPUUtilization_arr.append(datapoints['Maximum']);

                np_CPUUtilization_arr = np.array(CPUUtilization_arr)
                specific_Percentile = round(np.percentile(np_CPUUtilization_arr, Percentile_To_Check),1) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (InstanceID + "  Max EC2 CPUUtilization P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile))


        except:
            csv_arr.append("No CPUUtilization datapoints");


        try:

            NetworkIn = CollectMetric(InstanceID,MetricNamespace,'NetworkIn','Maximum','InstanceId',Metric_Period_Total,start_time,end_time);
            NetworkIn_1 = CollectMetric(InstanceID,MetricNamespace,'NetworkIn','Maximum','InstanceId',Metric_Period,period_1_start_time,period_1_end_time);
            NetworkIn_2 = CollectMetric(InstanceID,MetricNamespace,'NetworkIn','Maximum','InstanceId',Metric_Period,period_2_start_time,period_2_end_time);


            if NetworkIn['Datapoints']:
                for datapoints in NetworkIn_1['Datapoints']:

                    NetworkIn_arr.append(datapoints['Maximum']);

                for datapoints in NetworkIn_2['Datapoints']:

                    NetworkIn_arr.append(datapoints['Maximum']);

                np_NetworkIn_arr = np.array(NetworkIn_arr)
                specific_Percentile = round(np.percentile(np_NetworkIn_arr, Percentile_To_Check)/1024,1) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (InstanceID + "  EC2 Max NetworkIn P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile) + " KB")

        except:
            csv_arr.append("No NetworkIn datapoints");



        try:

            NetworkOut = CollectMetric(InstanceID,MetricNamespace,'NetworkOut','Maximum','InstanceId',Metric_Period_Total,start_time,end_time);
            NetworkOut_1 = CollectMetric(InstanceID,MetricNamespace,'NetworkOut','Maximum','InstanceId',Metric_Period,period_1_start_time,period_1_end_time);
            NetworkOut_2 = CollectMetric(InstanceID,MetricNamespace,'NetworkOut','Maximum','InstanceId',Metric_Period,period_2_start_time,period_2_end_time);


            if NetworkOut['Datapoints']:
                for datapoints in NetworkOut_1['Datapoints']:

                    NetworkOut_arr.append(datapoints['Maximum']);

                for datapoints in NetworkOut_2['Datapoints']:

                    NetworkOut_arr.append(datapoints['Maximum']);

                np_NetworkOut_arr = np.array(NetworkOut_arr)
                specific_Percentile = round(np.percentile(np_NetworkOut_arr, Percentile_To_Check)/1024,1) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (InstanceID + "  EC2 Max NetworkOut P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile) + " KB")

        except:
            csv_arr.append("No NetworkOut datapoints");


        return csv_arr;
    
    except botocore.exceptions.ClientError as error:
        raise error

    #except:
        #csv_arr.append(InstanceID + " doesn't not exist")
        #return csv_arr;


def Collect_EC2_Utilization():

    nextToken = None

    NotComplete = True

    EC2_Instance_arr = []

    csv = open(Result_fileout, "w")
    columnTitleRow = "InstanceId,InstanceType,InstanceLifecycle,ImageId,LaunchTime,Monitoring,AvailabilityZone,Tenancy,State,SubnetId,VpcId,Architecture,EBS_DeleteOnTermination,EbsOptimized,EnaSupport,Hypervisor,PSA,MetricCollectionStart,MetricCollectionEnd,CPUUtilization,NetworkIn,NetworkOut\n"
    csv.write(columnTitleRow)

    #csv_arr_all =[]

    while NotComplete:
        if nextToken:
            response_iterator = ec2client.describe_instances(
                Filters=[
                    {   
                        'Name': 'instance-state-name', 
                        'Values': ['running']
                    }
                ],
                MaxResults=1000,
                NextToken=nextToken
            )
        else:
            response_iterator = ec2client.describe_instances(
                Filters=[
                    {   
                        'Name': 'instance-state-name', 
                        'Values': ['running']
                    }
                ],
                MaxResults=1000
            )
        
        for EC2_Reservation in response_iterator['Reservations']:
            for EC2_Instance in EC2_Reservation["Instances"]:
                #if key_to_find not in [tag['Key'] for tag in EC2_Instance['Tags']]:
                EC2_Instance_arr.append(EC2_Instance["InstanceId"])

        try:
            nextToken = response_iterator['NextToken']
        except KeyError:
            NotComplete = False

    for Instance in EC2_Instance_arr:

        csv_arr = GetEC2Metric(Instance);

        csv_arr.append("\n");
        csv.write(','.join(map(str, csv_arr)));

    print ("\n");
    print ("\n");
    print ("---------------------------------------------------------------------------------------");
    print ("Retrieving EC2 Utilization for "+str(daysTocheck_1)+" days [Completed]");	
    print ("Please refer '"+Result_fileout+"' for more details\n");
    csv.write("-----------------------------------------------------------------------------------------------\n");
    csv.write("-----------------------------------------------------------------------------------------------\n");


Collect_EC2_Utilization();