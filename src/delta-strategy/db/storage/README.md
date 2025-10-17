# Database Delta Storage - Visual Overview

This directory contains the database-centric implementation of delta storage using TypeORM with support for both PostgreSQL (production) and SQLite (testing).

## Architecture Overview

The database delta storage follows a different paradigm from file-based storage:

- **File-based storage**: Current data comes from live DataSource, only previous data is stored
- **Database storage**: Both current and previous data are stored, deltas computed via SQL operations

## Comprehensive Delta Workflow

This flowchart illustrates the complete delta processing workflow: data is pulled from the source, stored in the database, deltas are computed and pushed to the target system. Based on push results, the system either promotes current data to become the new baseline (all success) or replaces both tables with cleaned data after removing failed records (partial failure).

```mermaid
flowchart TD
    Pull[Pull current data from data source] -->
    Store[Insert pulled current data into database] -->
    Delta[Compute Delta<br/>added/updated/removed] -->
    Push[Push delta to data target] --> Results{Push Results}
    
    Results -->|All Success| Promote[Swap current table rows into previous table rows to &quot;promote&quot; as new baseline]
    
    Results -->|Partial Failure| Identify[Identify Failed Records]
    Identify --> CheckType{For each failed record:<br/>New or Existing?}
    
    CheckType -->|New Record| RemoveNew[Remove record from processed data array]
    CheckType -->|Existing Record| RestoreHash[Restore previous hash in processed data array]
    
    RemoveNew --> Cleaned{"&quot;Cleaned&quot;<br/>array"}
    RestoreHash --> Cleaned
    Cleaned --> ReplaceData

    
    
    ReplaceData[Replace both current and previous table rows with cleaned data]    
    Promote --> End
    ReplaceData --> End
    End --> |Cron interval...|Restart
    
    Restart --> Pull
```

## Delta Computation

This diagram expands on the "Store" step from the beginning of the Comprehensive Delta Workflow above, showing the detailed 4-step process of how current data is stored and rotated in the database and how deltas are computed using SQL joins between the Previous and Current tables.

```mermaid
graph TD
    Pull[📊 Data source]

    subgraph Store ["storeCurrentData"]
        EmptyPrev[1. Purge]
        CopyToPrev[2. Swap]
        EmptyCurr[3. Purge]
        CopyNew[4. Copy]
    end

    subgraph Database ["Database"]
        Previous[Previous<br/>Table]
        Current[Current<br/>Table]
        Join{inner/outer</br>joins}
        Delta[Delta added/updated/removed]
    end
    
    EmptyPrev --> |Delete all rows|Previous
    CopyToPrev --> |Insert all current<br/>table rows|Previous
    EmptyCurr --> |Delete all rows|Current
    CopyNew --> |Insert newly<br/>pulled data|Current

    Pull --> |New data|CopyNew
    Previous --> Join
    Current --> Join
    Delta --> Push[Send delta records to Target System]
    Join --> Delta

```

## SQL Delta Computation

This diagram details the SQL join operations from the "Join" step in the Delta Computation diagram above, showing the specific LEFT JOIN and INNER JOIN queries used to identify added, updated, and removed records by comparing primary keys and hash values between tables.

```mermaid
graph LR
    subgraph "Added Records"
        A1[Current Table] --> A2[LEFT JOIN Previous on PK]
        A2 --> A3[WHERE previous IS NULL]
    end
    
    subgraph "Updated Records"  
        U1[Current Table] --> U2[INNER JOIN Previous on PK]
        U2 --> U3[WHERE hash differs]
    end
    
    subgraph "Removed Records"
        R1[Previous Table] --> R2[LEFT JOIN Current on PK]
        R2 --> R3[WHERE current IS NULL]
    end
    
    A3 --> D[Delta Result]
    U3 --> D
    R3 --> D
```


