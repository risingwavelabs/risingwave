import __init__
import model

if __name__ == "__main__":
    with model.TrainingModelService() as servicer:
        servicer.serve()
