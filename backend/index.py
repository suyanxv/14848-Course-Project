from flask import Flask, render_template, request
import os
import re, time, json
import subprocess
from werkzeug.utils import secure_filename
from random import randint
from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc

UPLOAD_FOLDER = 'uploads/'
project_id = 'course-project-ii-suyanx'
region = 'us-central1'
cluster_name = 'cluster-b6a1'
JAR_FILE = 'file:///usr/lib/hadoop/hadoop-streaming.jar'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def upload_file():
   return render_template('upload.html')

# path to upload files into client side application folder
@app.route('/uploader', methods = ['GET', 'POST'])
def uploader():
   if request.method == 'POST':
        files = request.files.getlist('file')
        for f in files:
                path = os.path.join(os.getcwd(), os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f.filename)))
                f.save(path)
        # unzip and gunzip file, flatten directory
        subprocess.run(["./unzip.sh"])
        show = 'file uploaded successfully: ' + path + ',   ,  <br/> hihi: <br/>' + '<br/>'.join(os.listdir(os.path.join(os.getcwd(), app.config['UPLOAD_FOLDER'])))
        file_list = [f.filename for f in files]
        return render_template('upload.html', files = file_list)

# path to upload files to bucket, construct inverted indices, and merge/copy results to bucket	
@app.route('/constructor', methods = ['GET', 'POST'])
def constructor(): 
        # connect with the 2nd application in GCP
        storage_client = storage.Client()
        buckets = list(storage_client.list_buckets())
        print(buckets)
        bucket = storage_client.get_bucket('dataproc-staging-us-central1-454011530827-lsujzseq')

        # save data files to bucket
        directory = os.path.join(os.getcwd(), 'RawData/')
        for filename in os.listdir(directory):
                f = os.path.join(directory, filename)
                if os.path.isfile(f):
                        blob = bucket.blob('project/test_data/'+filename)
                        blob.upload_from_filename(f)

        # hadoop job request to construct inverted indices
        # reference on converting gcloud command into python code using the dataproc API: 
        # https://stackoverflow.com/questions/55566082/dataproc-submit-a-hadoop-job-via-python-client
        out_folder = 'out' + str(int(time.time()))
        cluster_client = dataproc.ClusterControllerClient(
                client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )
        job_client = dataproc.JobControllerClient(
                client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
        )
        job = {
                "placement": {"cluster_name": cluster_name},
                "hadoop_job": {
                "main_jar_file_uri": JAR_FILE,
                "file_uris": ['gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/project_mapper.py',
                                'gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/project_reducer.py'],
                "args": ['-input',
                        'gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/test_data/*',
                        '-output',
                        'gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/'+out_folder+'/',
                        '-mapper',
                        'python project_mapper.py',
                        '-reducer',
                        'python project_reducer.py'],
                },
        }
        operation = job_client.submit_job_as_operation(
                request={"project_id": project_id, "region": region, "job": job}
        )
        response = operation.result()
        # use regex to obtain the bucket and blob info
        # reference on parsing the output and download_as_string:
        # https://cloud.google.com/dataproc/docs/guides/submit-job
        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        output = (
                storage.Client()
                .get_bucket(matches.group(1))
                .blob(f"{matches.group(2)}.000000000")
                .download_as_string()
        )
        print(f"Hadoop job finished successfully: {output}")

        # pig job request tp run bash commands to copy reduce output files from bucket into cluster
        # and merge into single file, then copy merged file into bucket
        job_client = dataproc.JobControllerClient(
                client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
        )
        job = {
                "placement": {"cluster_name": cluster_name},
                "pig_job": {
                "query_list": {
                                "queries": [
                                        "sh touch temp",
                                        "sh rm -r *",
                                        "sh gsutil cp -r gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/"+out_folder+" .",
                                        "sh cat "+out_folder+"/part* > test_results",
                                        "sh gsutil cp test_results gs://dataproc-staging-us-central1-454011530827-lsujzseq/project"
                                ]
                                }
                },
        }
        operation = job_client.submit_job_as_operation(
                request={"project_id": project_id, "region": region, "job": job}
        )
        response = operation.result()
        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        output = (
                storage.Client()
                .get_bucket(matches.group(1))
                .blob(f"{matches.group(2)}.000000000")
                .download_as_string()
        )
        print(f"Pig job finished successfully: {output}")

        return render_template('construct.html', buckets = output)

@app.route('/search', methods = ['GET', 'POST'])
def search():
        return render_template('search.html')

# path to search a term(word) in all uploaded files
@app.route('/search_term', methods = ['GET', 'POST'])
def search_term():
        # communicate with 2nd application, get the execution time and frequencies
        start_time = int(time.time())
        term = request.form.get('term').lower()

        # pig job request to copy merge results and search.py into cluster
        # run search.py on results
        # copy search results to bucket
        job_client = dataproc.JobControllerClient(
                client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
        )
        job = {
                "placement": {"cluster_name": cluster_name},
                "pig_job": {
                "query_list": {
                                "queries": [
                                        "sh touch temp",
                                        "sh rm -r *",
                                        "sh gsutil cp gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/test_results .",
                                        "sh gsutil cp gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/search.py .",
                                        "sh python search.py "+term,
                                        "sh python search.py "+term+" > search_results",
                                        "sh gsutil cp search_results gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/"
                                ]
                                }
                },
        }
        operation = job_client.submit_job_as_operation(
                request={"project_id": project_id, "region": region, "job": job}
        )
        response = operation.result()
        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        output = (
                storage.Client()
                .get_bucket(matches.group(1))
                .blob(f"{matches.group(2)}.000000000")
                .download_as_string()
        )
        print(f"Pig job finished successfully: {output}")

        # Read search results from bucket and parsing into table on web app
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('dataproc-staging-us-central1-454011530827-lsujzseq')
        blob = bucket.get_blob('project/search_results')
        results = blob.download_as_string()
        print(results)
        frequencies = []
        if results.strip() != '':
                postings = results.split(b'\t')[1]
                postings = postings.replace(b"\'", b"\"")
                postings = json.loads(postings)
                total = results.split(b'\t')[2]
                
                i = 0
                for posting, count in postings.items():
                        if posting.strip() != '':
                                frequencies.append({
                                                        'Doc ID' : i,
                                                        'Doc Folder' : posting.split('/')[-2],
                                                        'Doc Name' : posting.split('/')[-1],
                                                        'Frequencies' : count
                                                })
                                i += 1
        end_time = int(time.time())
        duration = (end_time - start_time) * 1000
        return render_template('search_result.html', term = term, time = duration, frequencies = frequencies)

@app.route('/top', methods = ['GET', 'POST'])
def top():
        return render_template('top.html')

# path to return top n words with most occurrences
@app.route('/top_n', methods = ['GET', 'POST'])
def top_n():
        start_time = int(time.time())
        n = request.form.get('n', type=int)
        print(n)
        # pig job to copy results file and topn.py to cluster
        # run topn.py on results file
        # copy top n results to bucket
        job_client = dataproc.JobControllerClient(
                client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
        )
        job = {
                "placement": {"cluster_name": cluster_name},
                "pig_job": {
                "query_list": {
                                "queries": [
                                        "sh touch temp",
                                        "sh rm -r *",
                                        "sh gsutil cp gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/test_results .",
                                        "sh gsutil cp gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/topn.py .",
                                        "sh python topn.py "+str(n),
                                        "sh python topn.py "+str(n)+" > topn_results",
                                        "sh gsutil cp topn_results gs://dataproc-staging-us-central1-454011530827-lsujzseq/project/"
                                ]
                                }
                },
        }
        operation = job_client.submit_job_as_operation(
                request={"project_id": project_id, "region": region, "job": job}
        )
        response = operation.result()
        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        output = (
                storage.Client()
                .get_bucket(matches.group(1))
                .blob(f"{matches.group(2)}.000000000")
                .download_as_string()
        )
        print(f"Pig job finished successfully: {output}")

        # read top n results from bucket and parsing into table on web app
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('dataproc-staging-us-central1-454011530827-lsujzseq')
        blob = bucket.get_blob('project/topn_results')
        results = blob.download_as_bytes().decode("utf-8") 
        print(results)
        frequencies = []
        if results.strip() != '':
                result_list = results.split('\n')
                for item in result_list:
                        if item.strip() != '':
                                word = item.split('\t')[0]
                                total = int(item.split('\t')[-1])
                                frequencies.append({
                                        'Term': word,
                                        'Frequencies': total
                                })
        end_time = int(time.time())
        duration = (end_time - start_time) * 1000

        return render_template('top_result.html', frequencies = frequencies)

if __name__ == '__main__': 
#    app.run(debug = True)
        app.run(host ='0.0.0.0', port = 5001, debug = True) 
