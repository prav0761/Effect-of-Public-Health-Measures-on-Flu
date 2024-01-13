## Effect of Public Health Measured on Flu


Repository Structure
--------------------

    |- args           # args for sh file
    |- captions_transform# scripts for text augmentations
    |- image_transform# scripts for image augmentations
    |- dataset.py # scripts for dataset loaders
    |- eval_reproduce.py # scripts for reproducing results and evaluation
    |- finetune_main.py  # scripts for finetuning on flickr30k_dataset for cross modal tasks
    |- finetune_main_travel.py  # scripts for finetuning on flickr travel dataset for visaluzations img-sim, txt-img retrieval
    |- lars.py          # lars optimizer
    |- logger.py      # scripts for logger functions
    |- main.py   # scripts for pretraining on flickr30k(main process)
    |- metrics.py # scripts for loss functions, optimizer functions
    |- models.py       # backbone models and finetune models
    |- LICENSE          # license
    |- README.md        # the top level description of content and commands to reproduce results, data download instructions
    |- train_fns.py  # contains scripts for training, validation functions
    |- utils.py #   # scripts for helper functions and metrics calculation code
    
