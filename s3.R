read_s3_file = function(bucket, key, read_fun, uri_arg = 'input', ...) {
  read_args = list(...)
  file_uri = tempfile()
  read_args[[uri_arg]] = file_uri
  s3$download_file(Bucket = bucket, Key = key, Filename = file_uri)
  output = do.call('read_fun', read_args)
  file.remove(file_uri)  
  output
}

write_s3_file = function(x, bucket, key, write_fun, object_arg = 'x', uri_arg = 'file', ...) {
  write_args = list(...)
  file_uri = tempfile()
  write_args[[object_arg]] = x
  write_args[[uri_arg]] = file_uri
  do.call('write_fun', write_args)
  
  s3$upload_file(
    Filename = file_uri,
    Bucket = bucket,
    Key = key)
}

list_contents = function(objects, filter = NULL) {
  contents = sapply(objects$Contents, function(x) x$Key)
  if(is.null(filter)) {
    contents
  } else {
    contents[grepl(filter, contents)] 
  }
}
                    
get_timestamps = function(objects, filter = NULL) {
  contents = setNames(
    sapply(objects$Contents, function(x) x$LastModified),
    sapply(objects$Contents, function(x) x$Key))
  if(is.null(filter)) {
    contents
  } else {
    contents[grepl(filter, names(contents))] 
  }
}
