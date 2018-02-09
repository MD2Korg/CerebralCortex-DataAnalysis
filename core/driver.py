import sys
import os
import utils.config
import argparse


# ADD ADMISSION CoNTROL LOGIC



def process_features(feature_list):
    for module in feature_list:
        feature_class_name = getattr(module,'feature_class_name')
        feature_class = getattr(module,feature_class_name)
        feature_class_instance = feature_class()
        feature_class_instance.process()



def import_features(feature_list):
    feature_dir = os.path.join(utils.config.FEATURES_DIR_NAME)
    found_features = []
    if feature_list:
        feature_subdirs = feature_list
    else:
        feature_subdirs = os.listdir(feature_dir)
    
    for subdir in feature_subdirs:
        feature = os.path.join(feature_dir,subdir)
        if not os.path.exists(feature):
            print('Feature not found %s.' % subdir)
            continue
        if os.path.isdir(feature):
            print('Found feature %s' % feature)
            sys.path.append(feature)
            try:
                module = __import__(subdir)
                found_features.append(module)
            except Exception as exp:
                print exp
        print('AAAAAAAAAA')
    
    return found_features
    

def generate_feature_processing_order(feature_list):
    return feature_list

def main():
    # Get the list of the features to process
    parser = argparse.ArgumentParser(description='CerebralCortex Feature Processing Driver')
    parser.add_argument("-f", "--feature-list", help="List of feature names seperated by commas", required=False)
    args = vars(parser.parse_args())
    print('args %s' % args)
    feature_list = None
    if args['feature_list']:
        feature_list = args['feature_list'].split(',')
    
    found_features = import_features(feature_list)
    feature_to_process = generate_feature_processing_order(found_features)
    #process_features(feature_to_process)

        

    
if __name__ == '__main__':
    main()

