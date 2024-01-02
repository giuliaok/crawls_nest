from llama_cpp import Llama

def model(model_path, prompt, gpu = False, n_gpu_layers = None):
    if gpu:
        if n_gpu_layers is not None: 
            n_gpu_layers = 1
            print('n_gpu_layers automatically set to 1')
        llm = Llama(model_path = model_path, n_gpu_layers=n_gpu_layers)
    else: 
        llm = Llama(model_path=model_path)
    output = llm(prompt, max_tokens=512, echo=True)
    return (output["choices"][0]["text"])

def model_apply(df, prompt,  gpu = False):
    """
    Get industrial classification for websites  
    """
    prompt = 'what product is this website selling?'
    df['prediction'] = df['text'].apply(model, prompt)
    return df 

