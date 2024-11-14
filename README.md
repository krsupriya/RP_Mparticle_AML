# importer
This script's purpose is to accomodate a one time import into mparticle for hastings policies. It is not intended to be made available to non-tech people as this would need more work and testing in terms of UI/UX.

## Table of contents


1. [Get started](#getstarted)
1. [Usage](#usage)
1. [Configuration](#configuration)
   1. [Controlling how data is sent to mparticle](#flowcontrol)
   1. [MParticle configuration](#setup)
1. [CSV properties definition](#headers)
1. [Logging](#logging)

## Get started<a id="getstarted"></a>

- Make sure to use python 3.10 or higher
- Run `pip3 install -r requirements.txt`
- Create a folder named `data` at the root of this project
- Copy the csv file you want to import in that `data` folder
- Create a `.env` file at the root of the project and use the content from the template `.env_sample` (more informations in "configuration")

## Usage<a id="usage"></a>
- In the terminal at the root of the folder, type `python3 main.py` and follow instructions

It will require you ton confirm which file you want to start importing.

## Configuration<a id="configuration"></a>
As mentioned above, the `.env_sample` file has to be copied and renamed `.env`. Here are more details about that configuration file.

### Controlling how data is sent to mparticle<a id="flowcontrol"></a>
The two following properties are determining how the original file is sliced into smaller chunks.

- If the `MPARTICLE_IMPORT_SINGLE_BATCHES` flag is set to true, whatever the batch size, rows will be sent 1 by 1 to mparticle, resulting in a slower processing but the most granular if for some reason a row fails to go through.
- If the `MPARTICLE_IMPORT_SINGLE_BATCHES` flag is set to false, it will send as many rows as defined in `MPARTICLE_IMPORT_BATCH_SIZE` at a time, drastically reducing the time needed for the import, but potentially silently fails some row with wrong data (got to make 100% sure that the data in file is correct). That said there is a prior check that can be enforced to granularly check data and report it prior to sending it to mparticle. That is already the case for email.
- In normal conditions the the script will pause during a time defined in seconds in `SLEEP_BETWEEN_REQUESTS_SECONDS` before sending a request to mparticle. If you witness many retries it may be interesting to increase that value to give mparticle more time to process the batches.

```
MPARTICLE_IMPORT_BATCH_SIZE=100
MPARTICLE_IMPORT_SINGLE_BATCHES=false
SLEEP_BETWEEN_REQUESTS_SECONDS=0.01
```

### MParticle configuration<a id="setup"></a>
The following properties are dedicated to feed configuration. For the production import make sure the set debug to false, otherwise the console would be flooded with messages.
```
MPARTICLE_ENVIRONMENT=development
MPARTICLE_DATA_PLAN_NAME=main
MPARTICLE_DATA_PLAN_VERSION=3
MPARTICLE_API_KEY="the feed key"
MPARTICLE_API_SECRET="the feed secret"
MPARTICLE_DEBUG=false
MPARTICLE_HOST=https://s2s.eu1.mparticle.com/v2
```

## CSV properties definition<a id="headers"></a>
The file `headers_constants.py` contains csv headers references.

What you need to know:
- Adding/Removing one of those constants requires to make changes in the code
- The strings that are assigned there should be exactly matching the headers in the CSV
  - Capital/Lower case
  - Leading/Trailing spaces
  - ....
- Always verify that file before vs the headers in the provided csv (those constants must 100% match) 

## Logging<a id="logging"></a>

Logging of the progress is automatically recorded in logfiles created in `log` folder.

If for some reason the import stops, the last line of the last logfile will contain the starting row of the batch where the import stopped.

If you then try to launch the import again it will pick that value and require your input about whether you want to resume at that position or if you want to start from first row (or if you want to cancel).