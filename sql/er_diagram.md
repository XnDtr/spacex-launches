# ER Diagram

```mermaid
erDiagram
    rockets ||--o{ launches : "rocket_id"
    launchpads ||--o{ launches : "launchpad_id"
    launches ||--o{ launch_failures : "launch_id"
    launches ||--o{ launch_cores : "launch_id"
    cores ||--o{ launch_cores : "core_id"
    landpads ||--o{ launch_cores : "landpad_id"
    launches ||--o{ launch_capsules : "launch_id"
    capsules ||--o{ launch_capsules : "capsule_id"
    launches ||--o{ payloads : "launch_id"
    payloads ||--o{ payload_customers : "payload_id"
    payloads ||--o{ payload_nationalities : "payload_id"
    launches ||--o{ starlink : "launch_id"

    rockets {
        text rocket_id PK
        text name
        text type
        int active
    }
    launchpads {
        text launchpad_id PK
        text name
        real latitude
        real longitude
    }
    landpads {
        text landpad_id PK
        text name
        text type
    }
    capsules {
        text capsule_id PK
        text serial
        text status
    }
    cores {
        text core_id PK
        text serial
        int reuse_count
    }
    launches {
        text launch_id PK
        int flight_number
        text name
        text date_utc
        text rocket_id FK
        text launchpad_id FK
        int success
    }
    launch_failures {
        text launch_id FK
        int time_sec
        text reason
    }
    launch_cores {
        text launch_id FK
        text core_id FK
        text landpad_id FK
        int landing_success
    }
    launch_capsules {
        text launch_id FK
        text capsule_id FK
    }
    payloads {
        text payload_id PK
        text name
        text launch_id FK
        text orbit
        real mass_kg
    }
    payload_customers {
        text payload_id FK
        text customer
    }
    payload_nationalities {
        text payload_id FK
        text nationality
    }
    starlink {
        text starlink_id PK
        text launch_id FK
        real height_km
        real velocity_kms
        int decayed
    }
```

Rendered version: paste this file's contents into the [Mermaid Live Editor](https://mermaid.live) or view directly on GitHub, which renders ` ```mermaid ` code blocks natively in Markdown.
