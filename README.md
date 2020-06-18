# occupationcoder

## A tool to use job text, such as job description, to assign standard occupational classification codes

Given a job title, job description, and job sector the algorithm assigns a 3-digit standard occupational classification (SOC) code to the job. The algorithm uses the **SOC 2010** standard, more details of which can be found on [the ONS' website](https://www.ons.gov.uk/methodology/classificationsandstandards/standardoccupationalclassificationsoc/soc2010).

This code originally written by Jyldyz Djumalieva, [Arthur Turrell](http://aeturrell.github.io/home), David Copple, James Thurgood, and Bradley Speigner. If you use this code please cite:

Turrell, A., Speigner, B., Djumalieva, J., Copple, D., & Thurgood, J. (2019). [Transforming Naturally Occurring Text Data Into Economic Statistics: The Case of Online Job Vacancy Postings](https://www.nber.org/papers/w25837) (No. w25837). National Bureau of Economic Research.

```
@techreport{turrell2019transforming,
  title={Transforming naturally occurring text data into economic statistics: The case of online job vacancy postings},
  author={Turrell, Arthur and Speigner, Bradley and Djumalieva, Jyldyz and Copple, David and Thurgood, James},
  year={2019},
  institution={National Bureau of Economic Research}
}
```


### Pre-requisites
See requirements.txt for a full list. 

occupationcoder is built on top of [NLTK](http://www.nltk.org/) and uses 'Wordnet' (a corpora, number 82 on their list) and the Punkt Tokenizer Models (number 106 on their list). When the coder is run, it will expect to find these in their usual directories. If you have nltk installed, you can get them corpora using ```nltk.download()``` which will install them in the right directories or you can go to [http://www.nltk.org/nltk_data/](http://www.nltk.org/nltk_data/) to download them manually (and follow the install instructions). 

A couple of the other packages, such as [```fuzzywuzzy```](https://github.com/seatgeek/fuzzywuzzy), do not come with the Anaconda distribution of Python. You can install these via pip (if you have access to the internet) or download the relevant binaries and install them manually.

### File and folder description
* ```conda.recipe``` contains code which helps to install the package
* ```occupationcoder/coder``` applies SOC codes to job descriptions
* ```occupationcoder/createdictionaries``` turns the ONS' index of SOC code into dictionaries used by ```occupationcoder/coder```
* ```occupationcoder/dictionaries``` contains the dictionaries used by ```occupationcoder/coder```
* ```occupationcoder/outputs``` is the default output directory
* ```occupationcoder/testvacancies``` contains 'test' vacancies to run the code on
* ```occupationcoder/utilities``` contains helper functions which mostly manipulate strings

### Installation via terminal using pip
Download the package and cd to the download directory. Then use
```Terminal
python setup.py sdist
cd dist
pip install occupationcoder-version.tar.gz
```
The first line creates the .tar.gz file, the second navigates to the directory with the packaged code in, and the third line installs the package. The version number to use will be evident from the name of the .tar.gz file.

### Running the code as a python package
Importing, and creating an instance, of the coder
```Python
import pandas as pd
from occupationcoder.coder import coder
myCoder = coder.Coder()
```
To run the code on a single job, use the following syntax with the ```codejobrow(job_title,job_description,job_sector)``` method:
```Python
if __name__ == '__main__':
    myCoder.codejobrow('Physicist','Calculations of the universe','Professional scientific')
```
The ```if``` statement is required because the code is parallelised. Note that you can leave some of the fields blank and the algorithm will still return a SOC code.

To run the code on a file (eg csv name 'job_file.csv') with structure

| job_title     | job_description| job_sector |
| ------------- |:--------------| :----------|
| Physicist     | Make calculations about the universe, do research, perform experiments and understand the physical environment. | Professional, scientific & technical activities |

use
```Python
df = pd.read_csv('path/to/foo.csv')
df = myCoder.codedataframe(df)
```
This will return a new dataframe with SOC code entries appended in a new column:

| job_title     | job_description| job_sector | SOC_code |
| ------------- |:--------------| :----------| ------|
| Physicist     | Make calculations about the universe, do research, perform experiments and understand the physical environment. | Professional, scientific & technical activities | 211 |

### Running the code from the command line
If you have all the relevant packages in requirements.txt, download the code and navigate to the occupationcoder folder (which contains the README). Then run
```Python
python -m occupationcoder.coder.coder path/to/foo.csv
```
This will create a 'processed_jobs.csv' file in the outputs/ folder which has the original text and an extra 'SOC_code' column with the assigned SOC codes.

### Testing
The test matches to SOC are run on a file of example jobs, in this case job vacancies.
The code to run the test is
```
python -m occupationcoder.coder.coder occupationcoder/testvacancies/test_vacancies.csv
```
and the output is in the 'processed_jobs.csv' file in the outputs/ folder.

### Acknowledgements
We are very grateful to Emmet Cassidy for testing this algorithm.

### Disclaimer
This code is provided 'as is'. We would love it if you made it better or extended it to work for other countries. All views expressed are our personal views, not those of any employer.
