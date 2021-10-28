from flask import Flask, render_template, request
import os
from werkzeug.utils import secure_filename
from random import randint
from google.cloud import storage

UPLOAD_FOLDER = 'uploads/'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/favicon.ico')
def favicon(): 
    return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/')
def upload_file():
   return render_template('upload.html')
	
@app.route('/uploader', methods = ['GET', 'POST'])
def uploader():
   if request.method == 'POST':
        files = request.files.getlist('file')
        print(files)
        for f in files:
                path = os.path.join(os.getcwd(), os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f.filename)))
                f.save(path)
        show = 'file uploaded successfullyyyy: ' + path + ',   ,  <br/> hihi: <br/>' + '<br/>'.join(os.listdir(os.path.join(os.getcwd(), app.config['UPLOAD_FOLDER'])))
        file_list = [f.filename for f in files]
        return render_template('upload.html', files = file_list)
		
@app.route('/constructor', methods = ['GET', 'POST'])
def constructor(): 
        # here we connect with the 2nd application in GCP
        storage_client = storage.Client()
        buckets = list(storage_client.list_buckets())
        print(buckets)
        return render_template('construct.html', buckets = buckets)

@app.route('/search', methods = ['GET', 'POST'])
def search():
        return render_template('search.html')

@app.route('/search_term', methods = ['GET', 'POST'])
def search_term():
        term = request.form.get('term')
        # communicate with 2nd application, get the execution time and frequencies
        time = 24 # dummy
        frequencies = [
                {
                        'Doc ID':1,
                        'Doc Folder':'histories',
                        'Doc Name':'1kinghenryiv',
                        'Frequencies':169
                },
                {
                        'Doc ID':2,
                        'Doc Folder':'histories',
                        'Doc Name':'1kinghenryiv',
                        'Frequencies':160
                },
                {
                        'Doc ID':3,
                        'Doc Folder':'histories',
                        'Doc Name':'2kinghenryiv',
                        'Frequencies':179
                },
                {
                        'Doc ID':4,
                        'Doc Folder':'histories',
                        'Doc Name':'2kinghenryiv',
                        'Frequencies':340
                }
        ]
        return render_template('search_result.html', term = term, time = time, frequencies = frequencies)

@app.route('/top', methods = ['GET', 'POST'])
def top():
        return render_template('top.html')

@app.route('/top_n', methods = ['GET', 'POST'])
def top_n():
        n = request.form.get('n', type=int)
        print(n)
        frequencies = []
        for i in range (n):
                frequencies.append({
                        'Term': i,
                        'Frequencies': randint(0, 10)
                })
        return render_template('top_result.html', frequencies = frequencies)

if __name__ == '__main__': 
#    app.run(debug = True)
        app.run(host ='0.0.0.0', port = 5001, debug = True) 


'''

export FLASK_APP=index
export FLASK_ENV=development
flask run
docker build -t suyanxv/app1 .
docker run -d --name app1 -p 5001:5001 suyanxv/app1
http://0.0.0.0:5001/upload

'''
