# Automated Switching of Databricks Job Ownership


# Introduction
In a production environment many times databricks jobs are created by specific users of the databricks workspace. These jobs when triggered by an external agent or databricks itself run using the user who created the job. However, this is not desirable as the user who created the job might be designated to another project or leave the organization and hence his permissions might get revoked. In such cases, the job created would not run and would halt the progress in the production environment. In order to fix this issue, the databricks job created should run as a service principal. This script does this exactly, it registers a service principal in a databricks environment via the scim api and then using the databricks jobs and permissions api assigns the service principal owner access for the job. It also allocates manage access to the service principal for running the script/notebook tied to the job so that when the job is triggered, the service principal can run the script/notebook. This in turn removes the dependency on the user who firstly created the job

# Design and Implementation

Jobs provide a non-interactive way to run applications in an Azure Databricks cluster, for example, an ETL job or data analysis task that should run on a scheduled basis. Typically these jobs run as the user that created them, but this can have some limitations:

-   Creating and running jobs is dependent on the user having appropriate permissions.
-   Only the user that created the job has access to the job.
-   The user might be removed from the Azure Databricks workspace.

Using a service account—an account associated with an application rather than a specific user—is a common method to address these limitations. The following is a high-level overview of the tasks involved in the `Switch_Databricks_Job_Ownership.py` script :

1.  Create a service principal in Azure Active Directory.
2.  Create a personal access token (PAT) in Azure Databricks. You’ll use the PAT to authenticate to the Databricks REST API.
3.  Add the service principal as a non-administrative user to Azure Databricks using the Databricks SCIM API.
4.  Create a job in Azure Databricks by mentioning the notebook path of notebook it should run.
5.  Transfer ownership of the job to the service principal.
6.  Test the job by running it as the service principal.

[Further Reading - Detailed Resource](https://docs.microsoft.com/en-us/azure/databricks/tutorials/run-jobs-with-service-principals)