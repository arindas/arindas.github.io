+++
title = "About"
template = "about.html"
+++

I am Arindam Das. I specialize in distributed systems, deep learning inference,
and AI SaaS at scale.

My expertise enables me to architect and implement cloud-native software services across multiple domains. I primarily have
experience in medical imaging and diagnosis, and real-time document processing.

## Skill set

### Technical skills

- **Languages**: C, C++, Java, Python, Golang, Rust, Javascript, Typescript, SQL
- **Frameworks and Libraries**: Django, FastAPI, Tensorflow, Pytorch, Pynetdicom, React, Glommio, Tokio
- **Tools**: Git, Vim, Neovim, Awk, Sed
- **DevOps**: Linux, Nginx, Docker, Bash, Zsh, Github Actions, Gitlab CI, Terraform
- **Databases**: PostgreSQL, Microsoft SQL Server, MySQL, SQLite, GCP Cloud Firestore
- **Cloud**: AWS{S3, RDS, EC2, Lightsail}, Firebase, GCP{Instances, PubSub, Cloud Storage}, <br>
  and Azure{Instances, Blob Storage, Container Apps}

### Soft skills

Agile Software Development, Requirement Analysis, System Design, Technical Content Delivery

## Experience

<h3>MLOps Engineer, Medical Imaging AI Services, Claritas Healthtech (06/2020 - Present)</h3>

<b>Responsibilities</b>

- Provisioning infrastructure for researchers to train and experiment with deep learning models
- Developing distributed deep learning inference services and corresponding client web applications
- Deploying and maintaining said applications as Cloud services (Prev: Azure and GCP. Currently on AWS)

<b>Projects</b>

- <b>Event-driven API Call Gateway for Inference Services</b>

  - Supports reading medical images from different "sources" (e.g. AWS S3) processing them with configured
    Deep Learning Inference services, and sending them to specified "sinks" (e.g. AWS S3)
  - Designed to accomodate archives containing multiple images, composite media like videos,
    dicom multiframe etc. even when the inference service expects single frame images.
  - Designed in an event driven fashion to accomodate multiple pipelines with different stages:
    enqueue, archive extraction / video split, ingestion, inference, response.
  - Incorporates a custom task scheduler to make incremental progress on tasks in different pipelines,
    including retries for failed tasks and moving tasks from one pipeline to another
  - Tasks are internally modelled as state machines. This allows us to limit the number of IN_PROGRESS
    tasks for rate-limiting traffic to our backend enhancer services. It also allows us to prioritize
    or set timeouts for tasks based on which state they are in. Each state transition causes a side-effect.
  - Engineered for concurrency at every level - from concurrent task state transitions
    to splitting and parallelizing work for individual tasks containing composite media.
  - Tunable to limit concurrency based on environment constraints - including limiting concurrent
    API calls to inference service to prevent overload
  - Accomodates sync and async API call mechanisms to Inference Services with detailed
    audit keeping for every API call and retries for failed API calls.
  - Capable of integrating authentication at every API call boundary - from data interchange
    with sources and sinks to API calls to machine learning inference services.
  - _This solution enables rapid integration of our Machine Learning inference services with
    downstream user applications - including our Cloud Storage application._
  - In addition to workflows with a single ML API call, we implemented multi-stage processor pipelines
  - Our Processor integration implementation is network protocol agnostic, allowing us to support different
    processors with different protocols. We support HTTP/S as well as DICOM networking with the possibility
    to support other protocols like GraphQL and gRPC in the future.
  - We support multiple types of pipelines, ranging from SEQUENTIAL where a batch of images is sequentially
    processed by multiple stages, to CUMULATIVE - where a single stage receives the original input batch
    as well as the outputs of all the stages before it. We also support a FANOUT stage, where we can spawn
    multiple oneshot tasks to process the same input batch of images in parallel, with the different
    processors in each pipeline stage.
  - _This solution will enable us to integrate our **image enhancement and segmentation services** with
    **third-party image processors** in the same pipeline without requiring complex code integration_.

- <b>Cloud Image annotation solution with AI Image enhancement</b>

  - Supports ingesting images of various formats (DICOM, NIFTI, VIDEO, JPG, PNG)
    with support for compressed ZIP archives.
  - Supports anonymizing images before ingestions for privacy
  - Supports 2D Polygon, Ellipsoid and Rectangular ROI annotations on images
  - Supports specifying labels for each 2D ROI. Different modality (CT, MRI etc.) and
    anatomy (kidney, brain etc.) pairs have different set if allowed labels.
  - Enforces exclusive user access to same image with locking across server-client
    boundaries. This makes it safe in the face of multi-user concurrent access to images in
    the same study.
  - Stores images in Cloud Object Storage (AWS S3) and maintains image metadata and
    catalog in the database. Images are organized by Project, Study, Series and Image records.
    Supports Microsoft SQL server, MySQL, PostgreSQL, SQLite
  - Includes an event-driven scheduler and API call Gateway to make requests to
    Enhancer Machine Learning inference services, with support for retries on failures.
    (similar in nature to the aforementioned project)
  - Images are organized in Projects. Each project can configured with an Enhancer.
    Images uploaded to a project configured with an enhancer are automatically
    enhanced. Enhanced images are placed alongside original images.
  - Supports authorization and e-mail user invites for image annotators (i.e. doctors) at the project level.
  - _This solution enabled us to get diverse sensitive image datasets annotated by
    doctors from Singapore, USA and Brazil. These annotations were indispensable for
    in-house research and development on novel image enhancement and segmentation methods._

- <b>Cloud based file storage solution built using Object Storage platforms (GCS, AWS S3)</b>

  - Files are organized by Buckets and Objects, mirroring file organization in Object Storage platforms
  - Supports bucket creation, bucket level user access authorization; object upload, download and delete.
  - Initially built as a golang web service with google-cloud-sdk, Cloud Firestore database
    and Firebase Authentication, along with a React SPA Frontend.
  - Later ported to Django to make it Cloud Agnostic. Currently supports AWS S3 backed file storage
    and multiple databases for maintaining user, bucket and object metadata - including PostgreSQL,
    Microsoft SQL Server, MySQL and SQLite.
  - Records all user access requests (object upload, download, bucket object listing) for detailed audit records
  - User access request records are also used for event driven processing. Includes a Framework
    to filter on user access request records and dispatch pre-configured actions (including API calls)
  - _This solution enabled us to collaborate on sensitive datasets with over
    10 different medical institutions across UK, Singapore and Europe without
    providing access to our cloud infrastructure._
  - The user access request filter dispatch action Framework makes it possible to make API
    calls to our Gateway for files uploaded to a specific bucket and receive processed files
    in the same bucket. This effectively allows automatic image processing with inference APIs
    for files uploaded to specific buckets.
  - _This solution enabled us to rapidly demonstrate our Deep Learning diagnosis solutions to
    third-party companies without granting access to our infrastructure._

- <b>Distributed deep-learning based diagnosis on medical images for a variety of diseases</b>

  - Designed as an event-driven suite of microservices, in golang and python.
  - We employ golang for the web serving infrastructure and python for
    inference.
  - Google Cloud PubSub is used as the messaging layer.
  - Capable of integrating with dedicated inference servers like Torchserve,
    Nvidia Triton and Tensorflow Serving
  - We provide a React Dashboard for: Visualizing medical images, Requesting AI
    diagnosis, Tracking inference status and Viewing AI Medical Diagnosis
    reports
  - _Reduced turnaround time for a new disease prediction service deployment by
    10x, along with improved audit record keeping of all predicted reports._

- <b>Dedicated Inference services for research Proof-of-concepts</b>
  - Implemented as a Django user facing application and a inference server.
  - The Django application behaves as a sidecar for the inference server
  - The inference server is either implemented as a FastAPI service or a
    dedicated Torchserve server based on requirements.
  - _I productionized 15+ deep-learning models split across 6 different web
    services in a span of 2 years._

<h3>Solution Architect, DeepWrex Technologies (04/2018 - 06/2020)</h3>

<b>Responsibilities</b>

- Architect cloud based solutions for machine learning software services.
- Assist researchers in implementing deep learning research papers.
- Iterating from research PoC to production.

<b>Projects</b>

- <b>Real-time named entity recognition system for medical reports.</b>

  - In house, economic alternative to AWS Medical Comprehend which didn't exist
    at the time.
  - Implemented as a event-driven suite of C++ microservices, with intermediate
    data storage on AWS S3.
  - We used Apache Kafka (using rdkafka) as the messaging layer.
  - This solution enabled our consulting partner to make medical reports more
    accessible to patients.

- <b>Black and white image colourization system</b>
  - We implemented the Instance aware image colourization
    [paper](https://arxiv.org/abs/2005.10825) which was the state-of-the-art
    deep learning based image colourization paper at the time.
  - Used Torchserve inference server for scalable GPU inference and FastAPI for
    user facing web services.
  - Deployed on AWS on a GPU enabled EC2 instance. (g4dn.xlarge)
  - We also developed and launched a Flutter Client Application to Google
    Play Store with over 50 downloads in the first month.

<h3>Satellite Onboard Computer RTOS Research Student, KIITSAT (04/2018 - 05/2019)</h3>

- Designed a shell capable of spawning programs and organizing pipes within 200
  lines of C
- Developed a minimal kernel for armv6 (on Raspberry Pi 3) with basic memory
  management and TTL I/O.
- Trained a team of 8 fellow researchers on OS concepts and implementation
  details.
- Collaborated with 2 different student engineering departments for integrating
  with different Satellite subsystems

<!--
<h3>VR 3d Game Development Instructor Intern, CampK12 (03/2020 - 06/2020)</h3>

<b>Responsibilities</b>

- Teaching K12 students about game development which entailed:
  - Basic Programming using Javascript (Control flow, operators, functions, callbacks etc.)
  - Elementary Trigonometry for animating movements of game objects

I guided 5 different students through 8 game-dev projects over a period of 3
months. At the end of the course the students were able to independently
implement features and explore new concepts.

- [riakv](https://github.com/arindas/riakv): Append-only key-value store, with checksum based integrity. Supports in-memory & presistent usage.

- [elevate](https://github.com/arindas/elevate): Barebones zero dependency HTTP File upload server in Go.

- [bheap](https://github.com/arindas/bheap): A Rust generic binary max heap implementation. It allows dynamic
  definition of the comparison function for the underlying domain at runtime.
-->

## Education

- <h3>B.Tech in Computer Science and Engineering</h3>
  <b>KIIT University (06/2017 - 06/2021)</b>, <b>CGPA</b>: 9.44 / 10, <b>SGPA 8th Semester</b>: 9.69 / 10
