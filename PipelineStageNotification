import boto3
import json
import logging
import re
import os
import datetime
from datetime import datetime
import sys
import pandas as pd
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)
codepipeline = boto3.client('codepipeline')
pipelines_dict=codepipeline.list_pipelines()['pipelines']
HOOK_URL = "https://hooks.slack.com/services/%s" %  (os.environ['GENERAL_WEBHOOK_SECRET']) #GENERAL_WEBHOOK_SECRET this varaible saved seperatelly 
PIPES_URL="https://eu-west-1.console.aws.amazon.com/codesuite/codepipeline/pipelines/"

def parce_pipelines(data):
    pipeline_context_higher=[]
    pipeline_context=[]
    pipeline_names=[]
    pipeline_names_higher=[]
    for i in range(len(data)):
        if 'latestExecution' in data[i]['stageStates'][0].keys() and len(data[i]['stageStates'][0].keys()) <= 3:
            pipeline_names.append(data[i]['pipelineName'])
            pipeline_context.append(data[i])
        elif 'latestExecution' in data[i]['stageStates'][0].keys() and len(data[i]['stageStates'][0].keys()) > 3:
            pipeline_names_higher.append(data[i]['pipelineName'])
            pipeline_context_higher.append(data[i])
    pipeline_execution_id=[pipeline_context_higher[i]['stageStates'][0]['latestExecution']['pipelineExecutionId'] for i in range(len(pipeline_context_higher))]
    converted_to_hours=[[round((datetime.now() - pipeline_context_higher[z]['stageStates'][0]['actionStates'][0]['latestExecution']['lastStatusChange'].replace(tzinfo=None)).total_seconds()/3600), "Hours ago!"]  for z in range(len(pipeline_context_higher))]
    return pipeline_names+pipeline_names_higher, pipeline_execution_id, converted_to_hours, data

def get_pipeline_execution(data):
    return  [codepipeline.get_pipeline_execution(pipelineName=data[0][idx],pipelineExecutionId=data[1][idx]) for idx in range(len(data[0]))]

def lambda_handler(event, context):
    pipelinesto_checking=[]
    pipeline_execution_context=[]
    transition_desabled=[]
    
    for pipeline in pipelines_dict:pipeline_execution_context.append(codepipeline.get_pipeline_state(name=pipeline['name']))
    datas=parce_pipelines(pipeline_execution_context)
    pipeline_execution_stage=get_pipeline_execution(datas)
    
    for element in range(len(pipeline_execution_stage)):
        pipeline_names=pipeline_execution_stage[element]['pipelineExecution']['pipelineName']
        sts=pipeline_execution_stage[element]['pipelineExecution']['status']
        if sts != 'Succeeded' and sts != 'Cancelled' and sts != 'Stopped':pipelinesto_checking.append([pipeline_names,sts])
    labels_to_drop = [1,3]
    pipelines_to_checking_df=pd.DataFrame(pipelinesto_checking).rename(columns={0:'pipelineName',1:'pipelineStatus'})
    all_pipes_df=pd.DataFrame(datas).T.drop(columns=labels_to_drop, axis=1).rename(columns={0:'pipelineName',2:'Timestamp'})
    time_added_df=pipelines_to_checking_df.merge(all_pipes_df)
    
    for elements in range(len(datas[3])):
        for line in time_added_df['pipelineName']:
            if datas[3][elements]['pipelineName'] == line:
                for seg in range(len(datas[3][elements]['stageStates'])):
                    if datas[3][elements]['stageStates'][seg]['inboundTransitionState']['enabled'] == False:
                        transition_desabled.append([line,
                               datas[3][elements]['stageStates'][seg]['inboundTransitionState']['disabledReason'],
                               datas[3][elements]['stageStates'][seg]['stageName']])
    transition_desabled_df=pd.DataFrame(transition_desabled, columns=['pipelineName','disabledReason','TransitionState'])
    results_df=pd.merge(time_added_df, transition_desabled_df, on=['pipelineName'], how="outer").fillna('')
    results=[]
    
    for line in results_df.values.tolist():
        if line[2][0] >= 6:results.append(("<{0}{1}/view?region=eu-west-1|{1}> %s " % "{2}").format(PIPES_URL, line[0], line[1::]))
    nl = '\n'
    text = f"<!subteam^S04QLK720LC> {nl}{nl.join(results)}"
    slack_message = {'text': text, 'Attachment': "Notification! "}
    #print(slack_message)
    req = requests.post(HOOK_URL, json.dumps(slack_message))
