# s3-file-contents-iterator
Had an idea to slow stream a S3 object using range requests to ensure HTTP connections are closed if processing fails

There's a common enough problem when processing an S3 object where is the processing itself can fail for some
reason the underlying HTTP connection has to be forced to close. This is a common enough source of bugs in my 
experience so I wrote this which 'safely' gets bytes in manageable chunks without potentially leaving a HTTP
connection opening and degrading the whole S3 client. 

I don't really think there's enough value in it as you can just parse an input stream and take care to close
the connection, but may as well make it available. 