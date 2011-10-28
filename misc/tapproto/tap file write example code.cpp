// write raw bytes to FileOutputStream

void TapFile::_raw(google::protobuf::io::FileOutputStream* fos, char *raw, int bytes) {
    void *p;
    int l;
    while (bytes > 0) {
        fos->Next(&p,&l);
        int transfer = min(l, bytes);
        memcpy(p, raw, transfer);
        raw += transfer;
        bytes -= transfer;
    }
    fos->BackUp(l-transfer);
}

void TapFile::_raw(google::protobuf::io::FileOutputStream* fos, char *string) {
    _raw(fos, string, strlen(string));
}

// pad to the next sector boundary            

static const int Alignment = 512;

void TapFile::_pad(google::protobuf::io::FileOutputStream* fos, int remnant) {
    int padBytes = Alignment - 1 - ((fos->ByteCount() + Alignment-1) % Alignment);
    padBytes -= remnant;
    void *p;
    int l;
    fos->Next(&p,&l);
    memset(p, 0xFA, padBytes);
    fos->BackUp(l-padBytes);
}

void TapFile::_pad(google::protobuf::io::CodedOutputStream* cos, int remnant) {
    int padBytes = Alignment - 1 - ((fos->ByteCount() + Alignment-1) % Alignment);
    padBytes -= remnant;
    unsigned char pad[padBytes];
    memset(pad, 0xFA, padBytes);
    cos->WriteRaw(pad, padBytes);
}

QBStatus TapFile::write(const QBKey &key, const QBEvent &event)
{
    const char  *method = "TapFile::write()";
    QBStatus    status;
    string      s;
    
    if (!isOpen()) return(QB_STREAMNOTOPEN);
    if (_mode == QB_READ) return(QB_WRONGACCESSMODE);
    
    // setup header and trailer fields on the first record written
    
    if (_first_write) {
        
        // setup header
        
        _header.set_message_name(event.GetTypeName());
        _header.set_target_decomp_size(_target_block_size);
        
        // write signature and header
        
        _oStream = new google::protobuf::io::CodedOutputStream(_fileOStream);
        _oStream->WriteRaw("tapproto",8);
        _oStream->WriteRaw("head",4);
        _oStream->WriteRaw("none",4);
        _header.SerializeToCodedStream(_oStream);
        _pad(_oStream, 0);
        delete _oStream;
        _oStream = NULL;
        
        // copy the header to the trailer and fill in what we know
        
        _trailer.CopyFrom(_header);
        _trailer.set_first_key(key.data().c_str(), key.size());
        _trailer.set_message_count(0);
        _trailer.set_data_block_count(0);
        _trailer.set_uncompressed_bytes(0);
        _trailer.set_max_decomp_size(0);
        
        // add descriptors from imported .proto Files to the trailer
        
        for (int i=0; i < event.GetDescriptor()->file()->dependency_count(); i++) {
            const FileDescriptor *fd_p = event.GetDescriptor()->file()->dependency(i);
            FileDescriptorProto fdproto;
            fd_p->CopyTo(&fdproto);
            fdproto.SerializeToString(&s);
            _header.add_format_descriptor(s.data(), s.size());
        }
        
        // add the main File descriptor proto to the trailer
        
        FileDescriptorProto fdproto;
        event.GetDescriptor()->file()->CopyTo(&fdproto);
        fdproto.SerializeToString(&s);
        _header.add_format_descriptor(s.data(), s.size());
        
        // create the ostringstream for the index, write the index block header
        // this index can get really large, we'll want to automatically grow 
        // the data block size as the File gets larger to limit the growth of the index
        
        _upperIxOstringstream = new ostringstream(ostringstream::binary|ostringstream::app|ostringstream::out);
        _upperIxOstreamOStream = new google::protobuf::io::OstreamOutputStream(_upperIxOstringstream);
        _upperIxOStream = new google::protobuf::io::CodedOutputStream(_upperIxOstreamOStream);
        
        _upperIxOStream->WriteRaw("upix",4);
        _upperIxOStream->WriteRaw("none",4);
        
        // first-write preprocessing is done
        
        _first_write = false;
    }
    
    // do we have a current open data block?
    
    if (_dataOStream == NULL) {
        
        // setup the index entry
        
        _upper_index_entry.Clear();
        _upper_index_entry.set_first_key(key.data().c_str(), key.size());
        _upper_index_entry.set_data_offset(_fileOStream->ByteCount());
        _upper_index_entry.set_message_count(0);
        
        // write the data block header 
        
        _put(_fileOStream, "data");
        _put(_fileOStream, "gzip");
        
        // create the GzipOutputStream for the data block
        
        google::protobuf::io::GzipOutputStream::Options options;
        options.compression_level = 6;
        _dataGzipOStream = new google::protobuf::io::GzipOutputStream(_fileOStream, options);
        _dataOStream = new google::protobuf::io::CodedOutputStream(_dataGzipOStream);
        
        // create the in-memory stream for the lower index for this data block
        
        _lowerIxOstringstream = new ostringstream(ostringstream::binary|ostringstream::app|ostringstream::out);
        _lowerIxOstreamOStream = new google::protobuf::io::OstreamOutputStream(_lowerIxOstringstream);
        _lowerIxOStream = new google::protobuf::io::CodedOutputStream(_lowerIxOstreamOStream);
        
        _lowerIxOStream->WriteRaw("loix",4);
        _lowerIxOStream->WriteRaw("none",4);
    }
    
    // serialize the record
    
    uint64_t startOffset = _dataOStream->ByteCount();
    event.SerializeToCodedStream(_dataOStream);
    uint64_t endOffset = _dataOStream->ByteCount();
    
    // prepare and serialize the key for this record in the lower index
    
    _lower_index_entry.Clear();
    _lower_index_entry.set_key(key.data().c_str(), key.size());
    _lower_index_entry.set_message_size(endOffset - startOffset);
    _lower_index_entry.SerializeToCodedStream(_lowerIxOStream);
    
    // update the upper index entry and trailer
    
    _upper_index_entry.set_message_count(_upper_index_entry.message_count() + 1);
    _trailer.set_last_key(key.data().c_str(), key.size());
    
    // close out this data block if full
    
    if (_dataOStream->ByteCount() >= _target_block_size) {
        _flush();
    }
    
    return(QB_SUCCESS);
}

QBStatus TapFile::_flush()
{
    static const char   *method = "TapFile::_flush()";
    if (_dataOStream != NULL) {
        
        //
        // update the trailer
        //
        
        _trailer.set_data_block_count(_trailer.data_block_count() + 1);
        _trailer.set_message_count(_trailer.message_count() + _upper_index_entry.message_count());
        _trailer.set_uncompressed_bytes(_trailer.uncompressed_bytes() + 
                                        _dataOStream->ByteCount() + 
                                        _lowerIxOStream->ByteCount());
        if (_trailer.max_decomp_size() < _dataOStream->ByteCount())
            _trailer.set_max_decomp_size(_dataOStream->ByteCount());
        
        //
        // close out the data block
        //
        
        // delete the data CodedOutputStream to flush to the Gzip stream
        delete _dataOStream;
        _dataOStream = NULL;
        // close and delete the GzipOutputStream to flush to the underlying coded stream
        _dataGzipOStream->Close();
        delete _dataGzipOStream;
        _dataGzipOStream = NULL;
        // pad to the next sector boundary
        _pad(_fileOStream, 0);
        
        // update the size of this data block in the upper index
        _upper_index_entry.set_data_bytes(_fileOStream->ByteCount() - _upper_index_entry.data_offset());
        
        //
        // close out and serialize the lower index for this data block
        //
        
        // delete the coded output stream to flush its contents to the OstreamOutputStrean
        delete _lowerIxOStream;
        _lowerIxOStream = NULL;
        // delete the OstreamOutputStream to flush its contents to the ostringstream
        delete _lowerIxOstreamOStream;
        _lowerIxOstreamOStream = NULL;
        // extract the data - yes we're making an additional copy in a string here
        // because I couldnt figure out how to get to the underlying buffer
        string s = _lowerIxOstringstream->str();
        // now delete the ostringstream to free memory
        delete _lowerIxOstringstream;
        _lowerIxOstringstream = NULL;
        // output the index and pad to the next sector boundary
        _raw(_fileOStream, s.c_str(), s.length());
        _pad(_fileOStream, 0);
        
        // update the size of this lower index in the upper index
        _upper_index_entry.set_lower_index_bytes(_fileOStream->ByteCount() - _upper_index_entry.lower_index_offset());
        
        //
        // now write out the upper index entry for this data block
        //
        
        _upper_index_entry.SerializeToCodedStream(_upperIxOStream);        
    }
    
    return(QB_SUCCESS);
}

QBStatus TapFile::close()
{
    static const char   *method = "TapFile::close()";
    QBStatus            status;
    
    if (_fd == 0) return(QB_SUCCESS);
    
    if (_mode != QB_READ) {
        
        // flush out the last data block
        
        _flush();
        
        //
        // close out and serialize the whole upper index
        //
        // yes making an extra copy of the entire index in a string is a bad idea
        // but i had trouble getting to the actual buffer to copy
        //
        // this should probably actually go into a temporary File, and then get copied 
        // over with sendFile(). the data would usually all stay in the buffercache
        //
        
        // delete the coded output stream to flush its contents to the OstreamOutputStrean
        delete _upperIxOStream;
        _upperIxOStream = NULL;
        // delete the OstreamOutputStream to flush its contents to the ostringstream
        delete _upperIxOstreamOStream;
        _upperIxOstreamOStream = NULL;
        // extract the data 
        string s = _upperIxOstringstream->str();
        // now delete the ostringstream to free memory
        delete _upperIxOstringstream;
        _upperIxOstringstream = NULL;
        // output the wole index and pad to the next sector boundary
        _trailer.set_index_offset(_fileOStream->ByteCount());
        _raw(_fileOStream, s.c_str(), s.length());
        _pad(_fileOStream, 0);
        _trailer.set_index_bytes(_fileOStream->ByteCount() - _trailer.index_offset());
        
        //
        // write the trailer including signature at the end of the File
        //
        
        uint64_t trailerOffset = _fileOStream->ByteCount();
        
        _oStream = new google::protobuf::io::CodedOutputStream(_fileOStream);
        _oStream->WriteRaw("trai",4);
        _oStream->WriteRaw("none",4);
        _header.SerializeToCodedStream(_oStream);
        _pad(_oStream, 16); // pad to 16 bytes before the sector end
        _oStream->WriteLittleEndian64(trailerOffset);
        _oStream->WriteRaw("tapproto",8);
        delete _oStream;
        _oStream = NULL;
        
        //
        // final cleanup of the File
        //
        
        _fileOStream->Close();
        delete _fileOStream;
        _fileOStream = NULL;
    }
    else {
        int retval = ::close(_fd);
        if (retval < 0) {
            qb_logerr(QB_FAILURE, __FILE__, __LINE__, method,
                      "Error from ::close File:%s, mode:%d, errno: %d", 
                      _name.c_str(), _mode, errno);
            return(QB_FAILURE);
        }
    }
    
    _name       = "";
    _fd         = 0;
    _eof_flag   = false;
    
    return(QB_SUCCESS);
}
