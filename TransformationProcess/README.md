Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Execute 

1) dbt init to start a project
2) dbt run --project-dir "path_to_dbt folder"
3) You should run first the script to have the raw data, after that you can run dbt to transform the data
4) The validation script allows you to validate some aspects of the quality of the transformation. An interesting point is that the total duration in some cases is not similar to the sum of the times of the different stages. You can see images about this issue and the general status about the data. 



Note: 
1) It is possible to use docker and Kubernetes to run dbt but in this case I use the python environment
