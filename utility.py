import glob,re

def get_site_id(cursor,code_name):
  #Determine the id of the given codename
  cursor.execute('SELECT id FROM sites WHERE code_name = ?', [code_name])
  site_id = cursor.fetchone()
  if not site_id:
    return 0,'THE GIVEN site DOES NOT APPEAR TO BE IN THE DATABASE.'
  return 1, site_id[0]

def get_fileparts(filename):
  #Returns processing level (L2/L4), year, and site code_name
  parts = re.split('_',filename)
  code_name = parts[1]
  year = parts[2]
  level = parts[3] + '_' + parts[4]

  return level,year,code_name


def get_ameriflux_filenames(path):
  #Returns the filenames matching ameriflux naming standard in a directory
  # defined by `path`
  file_paths = glob.glob(path + 'AMF_*')
  files = [re.split('/',file_path)[-1] for file_path in file_paths]

  return files
