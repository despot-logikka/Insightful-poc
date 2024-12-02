# Insightful-poc
Repo for poc of focus time/distractors
### TODO
##### Prepare app column to be mapped in a specific number of values. All mappings need to be performed inside a categoryId. Perfect to have in the beggining 50 - 200 app names.

##### Create mehanism for mapping new values into Unknown

##### Estimate some avg number of apps per day - to have an intuition what will be the length of the created text of the transitions

##### Discuss about the problem, will all these approaches lead to finding distractor groups or finging people with similar behaviour?

##### Do we really want only distractors (doesn't matter from where) ex: VsCode(40min) -> Youtube(1min) and Adobe PS (35min) -> Youtube(2min) ----> Do we want these to be the same?

##### Heuristic? Create a mechanism which will detect a potential distractor (ex: between 2 long apps you have a small portion of time--->usually distractor) (if having 3 small groups in a row----burst behaviour) (always find most dominant app per day (msot used one) and the most frequent one(ex: youtube) and put a better focus on these transitions durring the feature creation) --- Discuss with Njegos about this+

#### --------------------------------------------25-10-2024----------------------------------------------
#### Check for quality of newly created dataset D/Nj

#### Agregation of same apps one next to each other D

#### Be aware of micro differences between apps (potential operation roundigs may result in small differenes) D

#### Check about mapping the apps? Nj

#### Check if elastic search support informstion retrival for whole index Nj

#### Rename apps which have activae status False to custom name (Concentration Lost) D

#### Clean repo/ Refactor code a bit/ Update Github (Nj/D)

#### Non working time? (Weekends, After job) with client discussion D/Nj ? After creatimg th dataset

#### Check for browsers the distributions for some more specific app use

#### Check for different os loggings

#### Perform statistics on newly created dataset such as avg duration, most frequent apps and some other descriptive ones to have better intuition of setting thresholds

#### Check for URLs where sites are NaN
# ------------------------------------------------ Heuristics -------------------------------------------

# Short app between 2 longer sequences often may reflect a short distraction
# Series of changes between apps may reflect some kind of multy-tasking
# Detecting Concentration Lost as a distractor because people do not work in thos moments
# Most frequent app or app with lowest avg duration may represent a distractor
# Calculate the longest apps (avg session or whole duration) and check for every app that's comming after that app --> if len is short for the second app it may be a distractor

