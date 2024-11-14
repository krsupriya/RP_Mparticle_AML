from os import listdir

def prompt_data_files():
  files = listdir('./data')
  print("============ FILES AVAILABLE ================")
  for i,f in enumerate(files):
    print(f"[{i}] ---> {f}")
  answer = input("Type the number corresponding to the file you want to upload\nAnswer:")
  try:
    filename = files[int(answer)]
    x = input(f"Import file {filename}? [Y/N]")
    if x.lower() != "y":
      exit()
  except:
    print('Wrong value selected, exiting')
    exit()

  return filename