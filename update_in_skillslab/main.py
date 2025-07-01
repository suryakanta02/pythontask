from pipelines.athlete_pipeline import AthleteDataPipeline

def main():
    """Main function to execute the data pipeline."""
    print("Starting athlete data pipeline...")
    pipeline = AthleteDataPipeline()
    pipeline.run_pipeline()
    print("Pipeline execution completed.")

if __name__ == "__main__":
    main()