# occupationcoder

## A tool to use job text, such as job description, to assign standard occupational classification codes

Given a job title, job description, and job sector the algorithm assigns a 3-digit standard occupational classification (SOC) code to the job. The algorithm uses the SOC 2010 standard, more details of which can be found on [the ONS' website](https://www.ons.gov.uk/methodology/classificationsandstandards/standardoccupationalclassificationsoc/soc2010).

This code originally written by Jyldyz Djumalieva, Arthur Turrell, and David Copple. If you use this code please cite:
"Pretty Vacant: Understanding mismatch in the UK labour market from the bottom-up"

### Installation via terminal using pip
Download the package and cd to the download directory. Then use
```Terminal
python setup.py sdist
cd dist
pip install occupationalcoder-version.tar.gz
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
myCoder.codejobrow('Physicist','Calculations of the universe','Professional scientific')
```

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
If you have all the relevant packages in requirements.txt, download the code and navigate to the occupationcoder folder. Then run
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

### To do
- Put any repeated code into functions which are run efficiently
- Option to use title alone, or just title and description
- Option to output a confidence score
- Storing the outcomes of successfully matched records within an instance (up to a limit) so that if the same records appear again they get matched more efficiently.
- Make the parallel options scalable (so that they are enabled with a single option, or even automatically)
- Make sure code complies with PEP/usual Python naming conventions
- Add more jobs to the test file
- Implement continuous-bag-of-words approach as an alternative (see 'The Evolving U.S. Occupational Structure' by Enghin Atalay, Phai Phongthiengtham, Sebastian Sotelo, and Daniel Tannenbaum)
