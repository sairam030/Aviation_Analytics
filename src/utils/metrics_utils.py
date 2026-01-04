"""
Comprehensive Pipeline Metrics and Logging Utilities
"""
from datetime import datetime
import boto3
from src.batch import config


def show_pipeline_metrics(
    pipeline_name: str,
    task_ids: list,
    s3_buckets: list = None,
    **context
):
    """
    Show comprehensive pipeline metrics and statistics
    
    Args:
        pipeline_name: Name of the pipeline (e.g., "AVIATION STATES PIPELINE")
        task_ids: List of task IDs to track
        s3_buckets: List of tuples (layer_name, bucket_name) for storage stats
        context: Airflow context with ti, dag_run, etc.
    """
    ti = context['ti']
    dag_run = context['dag_run']
    
    print("\n" + "=" * 80)
    print(f"📊 {pipeline_name.upper()} - COMPREHENSIVE METRICS REPORT")
    print("=" * 80)
    
    # DAG Run Information
    print("\n🔄 DAG RUN INFORMATION")
    print("-" * 80)
    print(f"  DAG ID:           {dag_run.dag_id}")
    print(f"  Run ID:           {dag_run.run_id}")
    print(f"  Execution Date:   {dag_run.execution_date}")
    print(f"  Start Date:       {dag_run.start_date}")
    print(f"  State:            {dag_run.state}")
    
    # Calculate total duration
    total_duration = 0
    if dag_run.start_date:
        current_time = datetime.now(dag_run.start_date.tzinfo)
        total_duration = (current_time - dag_run.start_date).total_seconds()
        print(f"  Duration So Far:  {total_duration:.2f}s ({total_duration/60:.2f}m)")
    
    # Task-level metrics
    print("\n📋 TASK EXECUTION METRICS")
    print("-" * 80)
    
    total_task_time = 0
    successful_tasks = 0
    failed_tasks = 0
    
    for task_id in task_ids:
        try:
            task_instance = dag_run.get_task_instance(task_id)
            if task_instance and task_instance.start_date:
                if task_instance.end_date:
                    duration = (task_instance.end_date - task_instance.start_date).total_seconds()
                    total_task_time += duration
                    status_icon = "✓" if task_instance.state == "success" else "✗"
                    if task_instance.state == "success":
                        successful_tasks += 1
                    else:
                        failed_tasks += 1
                    print(f"  {status_icon} {task_id:35s} | {duration:7.2f}s | {task_instance.state}")
                else:
                    print(f"  ⏳ {task_id:35s} | Running...")
            else:
                print(f"  ⏳ {task_id:35s} | Not Started")
        except Exception as e:
            print(f"  ⚠️  {task_id:35s} | Unable to fetch metrics")
    
    if total_task_time > 0:
        print(f"\n  Total Task Time:     {total_task_time:.2f}s ({total_task_time/60:.2f}m)")
        print(f"  Successful Tasks:    {successful_tasks}/{len(task_ids)}")
        if failed_tasks > 0:
            print(f"  Failed Tasks:        {failed_tasks}")
    
    # Data Layer Statistics
    if s3_buckets:
        print("\n💾 DATA LAYER STATISTICS")
        print("-" * 80)
        
        try:
            # Create boto3 S3 client for MinIO
            s3_client = boto3.client(
                's3',
                endpoint_url='http://minio:9000',
                aws_access_key_id=config.MINIO_ACCESS_KEY,
                aws_secret_access_key=config.MINIO_SECRET_KEY,
                region_name='us-east-1'
            )
            
            for layer_name, bucket_name in s3_buckets:
                try:
                    response = s3_client.list_objects_v2(Bucket=bucket_name)
                    if 'Contents' in response:
                        total_size = sum(obj['Size'] for obj in response['Contents'])
                        file_count = len(response['Contents'])
                        print(f"\n  {layer_name}: {bucket_name}")
                        print(f"    Files:       {file_count}")
                        print(f"    Total Size:  {total_size / (1024*1024):.2f} MB")
                        
                        # Show file distribution by path
                        paths = {}
                        for obj in response['Contents']:
                            path_parts = obj['Key'].split('/')
                            base_path = '/'.join(path_parts[:-1]) if len(path_parts) > 1 else 'root'
                            paths[base_path] = paths.get(base_path, 0) + 1
                        
                        print(f"    Paths:")
                        for path, count in sorted(paths.items())[:5]:
                            print(f"      • {path}: {count} files")
                        
                        if len(paths) > 5:
                            print(f"      ... and {len(paths) - 5} more paths")
                    else:
                        print(f"\n  {layer_name}: {bucket_name}")
                        print(f"    No files found")
                except Exception as e:
                    print(f"\n  {layer_name}: {bucket_name}")
                    print(f"    ⚠️  Error: {str(e)}")
        except Exception as e:
            print(f"  ⚠️  Unable to connect to MinIO: {str(e)}")
    
    # Data Processing Metrics (from XCom)
    print("\n📈 DATA PROCESSING METRICS")
    print("-" * 80)
    
    xcom_found = False
    for task_id in task_ids:
        try:
            metadata = ti.xcom_pull(task_ids=task_id)
            if metadata and isinstance(metadata, dict):
                xcom_found = True
                print(f"\n  Task: {task_id}")
                for key, value in metadata.items():
                    if isinstance(value, (int, float)):
                        print(f"    • {key:30s}: {value:,}" if isinstance(value, int) else f"    • {key:30s}: {value:.2f}")
                    else:
                        print(f"    • {key:30s}: {value}")
        except Exception:
            pass
    
    if not xcom_found:
        print("  No processing metrics available (tasks may not push XCom data)")
    
    # Performance Summary
    print("\n⚡ PERFORMANCE SUMMARY")
    print("-" * 80)
    
    if total_task_time > 0:
        print(f"  Pipeline Efficiency:  {(total_task_time/total_duration)*100:.1f}% (task time / wall clock time)")
        print(f"  Avg Task Duration:    {total_task_time/len(task_ids):.2f}s")
    
    # System Health
    print("\n🏥 SYSTEM HEALTH CHECK")
    print("-" * 80)
    
    try:
        # Check MinIO
        s3_client = create_s3_client()
        buckets = s3_client.list_buckets()
        print(f"  MinIO Storage:    ✅ Healthy ({len(buckets.get('Buckets', []))} buckets)")
    except Exception as e:
        print(f"  MinIO Storage:    ❌ Unavailable - {str(e)}")
    
    # Check Spark (if applicable)
    spark_tasks = [t for t in task_ids if 'spark' in t.lower() or 'extract' in t.lower() or 'enrich' in t.lower()]
    if spark_tasks:
        spark_success = all(
            dag_run.get_task_instance(t).state == 'success' 
            for t in spark_tasks 
            if dag_run.get_task_instance(t) and dag_run.get_task_instance(t).state
        )
        print(f"  Spark Cluster:    {'✅ Healthy' if spark_success else '⚠️  Check logs'}")
    
    # Check PostgreSQL (if applicable)
    postgres_tasks = [t for t in task_ids if 'postgres' in t.lower() or 'create_' in t.lower()]
    if postgres_tasks:
        postgres_success = any(
            dag_run.get_task_instance(t).state == 'success' 
            for t in postgres_tasks 
            if dag_run.get_task_instance(t) and dag_run.get_task_instance(t).state
        )
        print(f"  PostgreSQL:       {'✅ Healthy' if postgres_success else '⚠️  Check logs'}")
    
    print("\n" + "=" * 80)
    print(f"✅ {pipeline_name.upper()} EXECUTION COMPLETE")
    print("=" * 80 + "\n")
    
    return {
        "status": "SUCCESS" if failed_tasks == 0 else "PARTIAL_SUCCESS",
        "total_duration": total_duration,
        "total_task_time": total_task_time,
        "successful_tasks": successful_tasks,
        "failed_tasks": failed_tasks,
        "timestamp": datetime.now().isoformat()
    }


def log_task_start(task_name: str, **kwargs):
    """Log task start with consistent formatting"""
    print("\n" + "=" * 80)
    print(f"🚀 STARTING TASK: {task_name}")
    print("=" * 80)
    print(f"  Timestamp: {datetime.now().isoformat()}")
    if kwargs:
        print("  Parameters:")
        for key, value in kwargs.items():
            print(f"    • {key}: {value}")
    print()


def log_task_end(task_name: str, **kwargs):
    """Log task completion with consistent formatting"""
    print("\n" + "-" * 80)
    print(f"✅ COMPLETED TASK: {task_name}")
    if kwargs:
        print("  Results:")
        for key, value in kwargs.items():
            print(f"    • {key}: {value}")
    print("-" * 80 + "\n")


def log_progress(message: str, level: str = "INFO"):
    """Log progress with consistent formatting"""
    icons = {
        "INFO": "ℹ️",
        "SUCCESS": "✅",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "PROCESSING": "⚙️"
    }
    icon = icons.get(level, "•")
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"  [{timestamp}] {icon} {message}")
