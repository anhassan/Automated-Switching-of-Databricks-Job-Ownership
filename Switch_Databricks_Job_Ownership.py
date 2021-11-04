# Databricks notebook source
# Register service principal via post request to the workspace
def register_service_principal(url,headers,payload,service_principal_name):
  try:
    requests.post(url,headers=headers,data=payload)
    print("Service Principal with service principal name : {} registered in databricks".format(service_principal_name))
  except Exception as error:
    print("Request Error : ",error)
    return []

# Add service principal to the databricks workspace
def add_service_principal_in_workspace(databricks_instance,databricks_auth_token,service_principal_client_id,service_principal_name):
  
  headers = {"Authorization": "Bearer " + databricks_auth_token}
  service_principal_base_url = "https://{}/api/2.0/preview/scim/v2/ServicePrincipals".format(databricks_instance)
  payload_service_principal =  json.dumps({"schemas":[
        "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"
      ],
      "applicationId": service_principal_client_id,
      "displayName": service_principal_name,
      "entitlements":[
        {
          "value":"allow-cluster-create"
        }
      ]})
  # check whether service principal exists in the databricks workspace already or not
  try:
      response = json.loads(requests.get(service_principal_base_url,headers=headers).content)
  except Exception as error:
    print("Request Error : ",error)
    return []
    
  if 'Resources' in response.keys():
    response_id_list = [service_principal['id'] for service_principal in response['Resources'] if service_principal['displayName'] == service_principal_name]
    if len(response_id_list) < 0 :
      # register the service principal in the databricks workspace as it is not registered before
      register_service_principal(service_principal_base_url,headers,payload_service_principal,service_principal_name)
        
    else:
      print("Service Principal with service principal name : {} already registered in databricks....".format(service_principal_name))
  else:
    register_service_principal(service_principal_base_url,headers,payload_service_principal,service_principal_name)
  

# COMMAND ----------

# Unregister service principal from databricks workspace given the service principal name
def unregister_service_principal(databricks_instance,databricks_auth_token,service_principal_name):
  
  headers={"Authorization": "Bearer " + databricks_auth_token}
  get_service_principal_url = "https://{}/api/2.0/preview/scim/v2/ServicePrincipals".format(databricks_instance)
  try:
      registered_service_principal = json.loads(requests.get(get_service_principal_url,headers=headers).content)['Resources']
  except Exception as error:
      print("Request Error : ",error)
      return []
  try:
      service_principal_id = [service_principal['id'] for service_principal in registered_service_principal if service_principal['displayName'] == service_principal_name][0]
  except Exception as error:
      print ("No service principal registered in databricks workspace named : {}....".format(service_principal_name))
      return []
  try:
      unregister_service_principal_url = "https://{}/api/2.0/preview/scim/v2/ServicePrincipals/{}".format(databricks_instance,service_principal_id)
      requests.delete(unregister_service_principal_url,headers=headers)
      print("Unregistered service principal : {} from databricks workspace".format(service_principal_name))
  except Exception as error:
      print("Request Error : ",error)
      return []
    

# COMMAND ----------

# Grant notebook tied to the input job can_manage permissions
def grant_job_notebook_permissions(headers,databricks_instance,service_principal_client_id,notebook_path):
  
  get_notebook_id_url = "https://{}/api/2.0/workspace/list?path={}".format(databricks_instance,notebook_path)
  try:
      notebook_id = json.loads(requests.get(get_notebook_id_url,headers=headers).content)['objects'][0]['object_id']
  except Exception as error:
      print("Request Error : ",error)
      return []
  payload_notebook = json.dumps({
  "access_control_list" : [{
    "service_principal_name" : service_principal_client_id,
    "permission_level" : "CAN_MANAGE" 
    }] 
  })
  change_notebook_permissions_url = "https://{}/api/2.0/preview/permissions/notebooks/{}".format(databricks_instance,notebook_id)
  try:
      requests.put(change_notebook_permissions_url,headers=headers,data=payload_notebook)
      print("Permission : CAN_MANAGE granted to {}".format(notebook_path))
  except Exception as error:
      print("Request Error :",error)
      return []
    

# COMMAND ----------

# Gives service principal is_owner permission on databricks job when provided the databricks job name along with service principal client id
def switch_databricks_job_ownership(databricks_instance,databricks_auth_token,job_name,service_principal_client_id,service_principal_name="service-principal-workspace"):
  
  headers = {"Authorization": "Bearer " + databricks_auth_token}
  
  # Add service principal to the workspace if its not added already
  add_service_principal_in_workspace(databricks_instance,databricks_auth_token,service_principal_client_id,service_principal_name)
  
  list_jobs_url = "https://{}/api/2.1/jobs/list".format(databricks_instance)
  try:
      list_jobs_response = json.loads(requests.get(list_jobs_url,headers=headers).content)['jobs']
  except Exception as error:
      print("Request Error : ",error)
      return []
  try:
      job_id = [job_contents['job_id'] for job_contents in list_jobs_response if job_contents['settings']['name'] == job_name][0]
  except Exception as error:
      print("No job with job name : {} exists...".format(job_name))
      return []
  try:
      job_id_query = "https://{}/api/2.1/jobs/get?job_id={}".format(databricks_instance,job_id)
      notebook_path = json.loads(requests.get(job_id_query,headers=headers).content)['settings']['tasks'][0]['notebook_task']['notebook_path']
  except Exception as error:
      print("Request Error : ",error)
      return []
  
  change_ownership_url = "https://{}/api/2.0/preview/permissions/jobs/{}".format(databricks_instance,job_id)
  payload_change_ownership = json.dumps({
  "access_control_list" : [{
    "service_principal_name" : service_principal_client_id,
    "permission_level" : "IS_OWNER" 
    
    }] 
  })
  try:
      requests.put(change_ownership_url,headers=headers,data=payload_change_ownership)
      print("Service principal given IS_OWNER rights for databricks job for job name : {}".format(job_name))
  except Exception as error:
      print("Request Error : ",error)
      return []
  
  # Grant service principal rights to run the notebook
  grant_job_notebook_permissions(headers,databricks_instance,service_principal_client_id,notebook_path)
  

# COMMAND ----------

# Driver Code
import json
import requests

# Get databricks secrets and metadata information
databricks_auth_token = dbutils.secrets.get(scope="switch-ownership",key="databricks-auth-token")
service_principal_client_id = dbutils.secrets.get(scope="switch-ownership",key="service-principal-client-id")
databricks_instance = "adb-8256961986085813.13.azuredatabricks.net"
job_name = "driver_trigger_notebook"
service_principal_name = "service-principal-workspace"

# Unregister the service principal tied with databricks workspace
unregister_service_principal(databricks_instance,databricks_auth_token,service_principal_name)
# Run the switch databricks job ownership 
switch_databricks_job_ownership(databricks_instance,databricks_auth_token,job_name,service_principal_client_id)

