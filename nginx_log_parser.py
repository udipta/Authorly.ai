import os
import re
import json
import pandas as pd


def extract_log_content(log_str: str = ''):
    """
        Functionality to filtered log content based on the regular expression
        and store into dictionary.

        ---
        :param
            - name: log_str
              desc: log string consisting info of nginx log
              required: True

        :return: 
            - name: log_dict
              desc: parsed dictionary consisting info of nginx log
    """
    
    parsed_log_dict = dict()
    
    try:
        
        # regex check to parse only valid POST/GET log string
        if re.search(r"(POST \/.+ \{.+\})|(GET \/.+\?.+=.+&)", log_str):
            
            # parser pattern to extract necessary required info from the log string
            parser_string = '$datetime header - "$body_bytes" [$date_time_str] $request $request_path $content_body "Status code :"$status_code  "Response Size :"$response_size "Response time :" $response_time ms'
            
            # structuring the regular expression using the parser pattern
            regex = ''.join('(?P<' + g + '>.*?)' if g else re.escape(c) for g, c in re.findall(r'\$(\w+)|(.)', parser_string))
            
            # validate regex pattern with the log string
            matchObject = re.match(regex, log_str)
            if matchObject:
                
                # extract dictionary from log string
                parsed_log_dict = matchObject.groupdict()
                
                # extract dictionary from log string
                    
                # flatten the content of nested dictionary
                content_body = json.loads(parsed_log_dict.get('content_body', '{}'))
                parsed_log_dict.pop('content_body')
                parsed_log_dict.update(content_body)
                
                # gather info from GET request logs
                if parsed_log_dict.get('request', '') == 'GET':
                    
                    # parse infos from request_path
                    request_path = parsed_log_dict.get('request_path', '')
                    request_path_split = parsed_log_dict.get('request_path', '').split('?')[-1].split('&')
                    if request_path_split:
                        parsed_log_dict.pop('request_path')
                        for element in request_path_split:
                            key,value = element.split('=')
                            if value:
                                parsed_log_dict[key]=value
    
    # exception handling
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        
    finally:
        return parsed_log_dict



if __name__ == "__main__":
    
    try:
        # define filepath
        log_file_path = 'nginx_log.txt'
        parsed_log_csv = 'nginx_log.csv'
        
        
        if os.path.isfile(log_file_path):
        
            # create DataFrame from the valid parsed logs
            log_df = pd.DataFrame([extract_log_content(line) for line in open(log_file_path).readlines() if extract_log_content(line)])
            
            # select the specific headers from the DataFrame
            log_df = log_df[['request', 'status_code', 'response_size', 'response_time', 'city_id', 
                     'change_latitude', 'current_latitude', 'type', 'newSearch','listType']]
            
            # write into file
            log_df.to_csv(parsed_log_csv, sep="|", index=False)
        else:
            print(f'FileNotFoundError!-{log_file_path}')

    # exception handling
    except Exception as e:
        import traceback
        print(traceback.format_exc())
    
