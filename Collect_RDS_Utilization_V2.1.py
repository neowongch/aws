#!/usr/bin/python


import boto3
import os, sys, csv, datetime, io, re, itertools, sys
import numpy as np
from datetime import datetime, timedelta


# encoding=utf8

#---------------------------------------------------------------------------------------------------------
# USAGE 	   	        : Collect RDS Utilization
# How to Run Example    : python Collect_RDS_Utilization.py "us-east-1"
# Thanks goes to ElasticFlo
#---------------------------------------------------------------------------------------------------------

#---------------------------------------------------------------------------------------------------------
#Global variables
#---------------------------------------------------------------------------------------------------------

daysTocheck_1 = 10;
daysTocheck_2 = 5;
Metric_Period = 300; #300s, 5 mins per datapoint
Metric_Period_Total = 600;
Percentile_To_Check = 99; 
MetricNamespace = 'AWS/RDS';


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
RDS_fileout = region+"_rds_utilization_"+timestamp+".csv";

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

rdsclient = boto3.client('rds',region_name=region);
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



def GetRDSMetric(RDS_ID):

    CPU_Ultization_arr =[];
    DatabaseConnections_arr =[];
    FreeableMemory_arr =[];
    FreeStorageSpace_arr =[];
    ReadIOPS_arr =[];
    WriteIOPS_arr =[];
    csv_arr =[];



    try:

        RDS_Info = rdsclient.describe_db_instances(DBInstanceIdentifier=RDS_ID);

        csv_arr.append(RDS_ID);
        csv_arr.append(RDS_Info['DBInstances'][0]['DBInstanceArn']);
        csv_arr.append(RDS_Info['DBInstances'][0]['DBInstanceClass']);
        csv_arr.append(RDS_Info['DBInstances'][0]['Engine']);
        csv_arr.append(RDS_Info['DBInstances'][0]['DBInstanceStatus']);
        csv_arr.append(RDS_Info['DBInstances'][0]['InstanceCreateTime']);
        csv_arr.append(RDS_Info['DBInstances'][0]['MultiAZ']);
        csv_arr.append(RDS_Info['DBInstances'][0]['AllocatedStorage']);
        csv_arr.append(RDS_Info['DBInstances'][0]['StorageType']);


        try:
            csv_arr.append(RDS_Info['DBInstances'][0]['Iops']);
        except:
            csv_arr.append("NA");
    
        csv_arr.append(start_time);
        csv_arr.append(end_time);

        print (RDS_ID)

        try:

            CPU_Ultization = CollectMetric(RDS_ID,MetricNamespace,'CPUUtilization','Maximum','DBInstanceIdentifier',Metric_Period_Total,start_time,end_time);
            CPU_Ultization_1 = CollectMetric(RDS_ID,MetricNamespace,'CPUUtilization','Maximum','DBInstanceIdentifier',Metric_Period,period_1_start_time,period_1_end_time);
            CPU_Ultization_2 = CollectMetric(RDS_ID,MetricNamespace,'CPUUtilization','Maximum','DBInstanceIdentifier',Metric_Period,period_2_start_time,period_2_end_time);


            if CPU_Ultization['Datapoints']:
                for datapoints in CPU_Ultization_1['Datapoints']:

                    CPU_Ultization_arr.append(datapoints['Maximum']);

                for datapoints in CPU_Ultization_2['Datapoints']:

                    CPU_Ultization_arr.append(datapoints['Maximum']);

                np_CPU_Ultization_arr = np.array(CPU_Ultization_arr)
                specific_Percentile = round(np.percentile(np_CPU_Ultization_arr, Percentile_To_Check),1) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (RDS_ID + "  RDS CPU_Ultization P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile))

        except:
            csv_arr.append("No CPU_Ultization datapoints");



        try:

            DatabaseConnections_max = CollectMetric(RDS_ID,MetricNamespace,'DatabaseConnections','Maximum','DBInstanceIdentifier',Metric_Period_Total,start_time,end_time);
            DatabaseConnections_max_1 = CollectMetric(RDS_ID,MetricNamespace,'DatabaseConnections','Maximum','DBInstanceIdentifier',Metric_Period,period_1_start_time,period_1_end_time);
            DatabaseConnections_max_2 = CollectMetric(RDS_ID,MetricNamespace,'DatabaseConnections','Maximum','DBInstanceIdentifier',Metric_Period,period_2_start_time,period_2_end_time);


            if DatabaseConnections_max['Datapoints']:
                for datapoints in DatabaseConnections_max_1['Datapoints']:

                    DatabaseConnections_arr.append(datapoints['Maximum']);

                for datapoints in DatabaseConnections_max_2['Datapoints']:

                    DatabaseConnections_arr.append(datapoints['Maximum']);

                np_DatabaseConnections_arr = np.array(DatabaseConnections_arr)
                specific_Percentile = round(np.percentile(np_DatabaseConnections_arr, Percentile_To_Check),0) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (RDS_ID + "  RDS Max DatabaseConnections P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile))

        except:
            csv_arr.append("No DatabaseConnections datapoints");


        try:

            FreeableMemory = CollectMetric(RDS_ID,MetricNamespace,'FreeableMemory','Minimum','DBInstanceIdentifier',Metric_Period_Total,start_time,end_time);
            FreeableMemory_1 = CollectMetric(RDS_ID,MetricNamespace,'FreeableMemory','Minimum','DBInstanceIdentifier',Metric_Period,period_1_start_time,period_1_end_time);
            FreeableMemory_2 = CollectMetric(RDS_ID,MetricNamespace,'FreeableMemory','Minimum','DBInstanceIdentifier',Metric_Period,period_2_start_time,period_2_end_time);


            if FreeableMemory['Datapoints']:
                for datapoints in FreeableMemory_1['Datapoints']:

                    FreeableMemory_arr.append(datapoints['Minimum']);

                for datapoints in FreeableMemory_2['Datapoints']:

                    FreeableMemory_arr.append(datapoints['Minimum']);

                np_FreeableMemory_arr = np.array(FreeableMemory_arr)
                specific_Percentile = round(np.percentile(np_FreeableMemory_arr, Percentile_To_Check)/(1024*1024*1024),1) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (RDS_ID + "  RDS Min FreeableMemory P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile))

        except:
            csv_arr.append("No FreeableMemory datapoints");


        try:

            FreeStorageSpace = CollectMetric(RDS_ID,MetricNamespace,'FreeStorageSpace','Minimum','DBInstanceIdentifier',Metric_Period_Total,start_time,end_time);
            FreeStorageSpace_1 = CollectMetric(RDS_ID,MetricNamespace,'FreeStorageSpace','Minimum','DBInstanceIdentifier',Metric_Period,period_1_start_time,period_1_end_time);
            FreeStorageSpace_2 = CollectMetric(RDS_ID,MetricNamespace,'FreeStorageSpace','Minimum','DBInstanceIdentifier',Metric_Period,period_2_start_time,period_2_end_time);


            if FreeStorageSpace['Datapoints']:
                for datapoints in FreeStorageSpace_1['Datapoints']:

                    FreeStorageSpace_arr.append(datapoints['Minimum']);

                for datapoints in FreeStorageSpace_2['Datapoints']:

                    FreeStorageSpace_arr.append(datapoints['Minimum']);

                np_FreeStorageSpace_arr = np.array(FreeStorageSpace_arr)
                specific_Percentile = round(np.percentile(np_FreeStorageSpace_arr, Percentile_To_Check)/(1024*1024*1024),1) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (RDS_ID + "  RDS Min FreeStorageSpace P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile))

        except:
            csv_arr.append("No FreeStorageSpace datapoints");


        try:
        
            ReadIOPS_max = CollectMetric(RDS_ID,MetricNamespace,'ReadIOPS','Maximum','DBInstanceIdentifier',Metric_Period_Total,start_time,end_time);
            ReadIOPS_max_1 = CollectMetric(RDS_ID,MetricNamespace,'ReadIOPS','Maximum','DBInstanceIdentifier',Metric_Period,period_1_start_time,period_1_end_time);
            ReadIOPS_max_2 = CollectMetric(RDS_ID,MetricNamespace,'ReadIOPS','Maximum','DBInstanceIdentifier',Metric_Period,period_2_start_time,period_2_end_time);


            if ReadIOPS_max['Datapoints']:
                for datapoints in ReadIOPS_max_1['Datapoints']:

                    ReadIOPS_arr.append(datapoints['Maximum']);

                for datapoints in ReadIOPS_max_2['Datapoints']:

                    ReadIOPS_arr.append(datapoints['Maximum']);

                np_ReadIOPS_arr = np.array(ReadIOPS_arr)
                specific_Percentile = round(np.percentile(np_ReadIOPS_arr, Percentile_To_Check),0) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (RDS_ID + "  RDS Max ReadIOPS P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile))

        except:
            csv_arr.append("No ReadIOPS datapoints");


        try:

            WriteIOPS_max = CollectMetric(RDS_ID,MetricNamespace,'WriteIOPS','Maximum','DBInstanceIdentifier',Metric_Period_Total,start_time,end_time);
            WriteIOPS_max_1 = CollectMetric(RDS_ID,MetricNamespace,'WriteIOPS','Maximum','DBInstanceIdentifier',Metric_Period,period_1_start_time,period_1_end_time);
            WriteIOPS_max_2 = CollectMetric(RDS_ID,MetricNamespace,'WriteIOPS','Maximum','DBInstanceIdentifier',Metric_Period,period_2_start_time,period_2_end_time);


            if WriteIOPS_max['Datapoints']:
                for datapoints in WriteIOPS_max_1['Datapoints']:

                    WriteIOPS_arr.append(datapoints['Maximum']);

                for datapoints in WriteIOPS_max_2['Datapoints']:

                    WriteIOPS_arr.append(datapoints['Maximum']);

                np_WriteIOPS_arr = np.array(WriteIOPS_arr)
                specific_Percentile = round(np.percentile(np_WriteIOPS_arr, Percentile_To_Check),0) # return 99th percentile.

                csv_arr.append(specific_Percentile);

                print (RDS_ID + "  RDS Max WriteIOPS P(" + str(Percentile_To_Check) + "): " + str(specific_Percentile))

        except:
            csv_arr.append("No WriteIOPS datapoints");



        return csv_arr;
    
    except:
        csv_arr.append(RDS_ID + " doesn't not exist")
        return csv_arr;


def Collect_RDS_Utilization():

    marker = None

    NotComplete = True

    RDS_Instance_arr = [];

    csv = open(RDS_fileout, "w");
    columnTitleRow = "DBInstanceIdentifier,DBInstanceArn,DBInstanceClass,Engine,DBInstanceStatus,InstanceCreateTime,MultiAZ,AllocatedStorage,StorageType,Iops,Metric_Start_Time,Metric_End_Time,Maximum_CPU_Ultization,MaximumDatabaseConnections,MininumFreeableMemory(GB),MinimumFreeStorageSpace(GB),MaximumReadIOPS,MaximumWriteIOPS\n";
    csv.write(columnTitleRow);


    print ("Retrieving RDS Utilization for "+str(daysTocheck_1)+" days [Started]");


    while NotComplete:
        if marker:
            response_iterator = rdsclient.describe_db_instances(
                MaxRecords=100,
                Marker=marker
            )
        else:
            response_iterator = rdsclient.describe_db_instances(
                MaxRecords=100
            )

        for RDS_Instance in response_iterator['DBInstances']:
            RDS_Instance_arr.append(RDS_Instance['DBInstanceIdentifier'])

        try:
            marker = response_iterator['Marker']
        except KeyError:
            NotComplete = False

 
    for RDS_DBInstanceIdentifier in RDS_Instance_arr:

        print(RDS_DBInstanceIdentifier)

        csv_arr = GetRDSMetric(RDS_DBInstanceIdentifier);

        csv_arr.append("\n");
        csv.write(','.join(map(str, csv_arr)));

    print ("\n");
    print ("\n");
    print ("---------------------------------------------------------------------------------------");
    print ("Retrieving RDS Utilization for "+str(daysTocheck_1)+" days [Completed]");	
    print ("Please refer '"+RDS_fileout+"' for more details\n");
    csv.write("-----------------------------------------------------------------------------------------------\n");
    csv.write("-----------------------------------------------------------------------------------------------\n");

    csv.close();



Collect_RDS_Utilization();
