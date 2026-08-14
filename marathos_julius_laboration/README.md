# New Pipeline 2026-05-22 13:34

This folder defines all source code for the 'New Pipeline 2026-05-22 13:34' pipeline:

- `explorations`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations`: All dataset definitions and transformations.
- `utilities`: Utility functions and Python modules used in this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Take a look at the sample under "sample_users_may_22_1334.py" to get familiar with the syntax.
  Read more about the syntax at https://docs.databricks.com/ldp/developer/python-ref.
* Use `Run file` to run and preview a single transformation.
* Use `Run pipeline` to run _all_ transformations in the entire pipeline.
* Use `+ Add` in the file browser to add a new data set definition.
* Use `Schedule` to run the pipeline on a schedule!

For more tutorials and reference material, see https://docs.databricks.com/ldp.



Bug found during gold EDA 

What i discovered:
'dim_event' had 79 793 rows compared to 79 560 unique rows in event_id.The cause was that, event_id is hashed using only event_name and event_date in the silver layer. This while multipel rows represent diffrent distance classes within the same event and date. 

**Exmaple** 
"Ultra Trail Ibiza(ESP) on 2017-12-02 had both an 85 km and a 46 km class. These were assigned the same event_id but had diffrent values for evnet_distance_length, event_distance_km and event_number_of_finsíshers. This caused the SELECT DISTINCT to added rows in the mart. 

**Decission**
I decided to keep the orignal event_id hash and instead solve the fan-out by aggregating dim_event by using FIRST() for the attributes. This may lead to a limitation when it comes to showing results per event, wich means dim_event only can show one distance class per event, rather then multiple. 