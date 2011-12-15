//
//  Tap Protobuf File I/O
//  Paul Sutter
//  Copyright (c) 2011 Quantbench Corporation
// 
//  need to convert all error handling to exceptions, add TapFileException with filename
//

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "tapfile.h"
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/descriptor.pb.h>

#include "tapexception.h"

using namespace     google;
using namespace     protobuf;

namespace tap {

static const int    DefaultTargetBlockSize   = 256*1024;
static string       _root_dir;

TapFile::TapFile() 
{
    TapException::establishTerminationHandler();
    
    //
    
    _source     = QB_TAPFILE;
    _fd         = 0;
    _metaFd     = 0;
    _indexFd    = 0;
    _name       = "";
    _eof_flag   = false;
    _target_block_size = DefaultTargetBlockSize;
    
    // yes i know the sheer number of streams is ridiculous
    // as is combining read and write paths in one file
    
    // null out the read path streams
    _metaIStream = NULL;
    _dataIStream = NULL;
    _metaFIStream = NULL;
    _dataGzipIStream = NULL;
    _dataFIStream = NULL;
    
    // null out the write path streams    
    _fileOStream = NULL;
    _oStream = NULL;
    _dataOStream = NULL;
    _indexOStream = NULL;
    _dataGzipOStream = NULL;
    _indexOstreamOStream = NULL;
    _indexOstringstream = NULL;
    
    _first_write = false;
    _message_count = 0;
    _data_block_count = 0;

    char *tap_data = getenv("TAPDATA");
    char *tap_root = getenv("TAPROOT");

    if (tap_data)  _root_dir = tap_data;
    else if (tap_root) _root_dir = tap_root + string("/data");
}

TapFile::~TapFile() 
{
    // free read path streams
    if (_metaIStream != NULL) delete _metaIStream;
    if (_dataIStream != NULL) delete _dataIStream;
    if (_metaFIStream != NULL) delete _metaFIStream;
    if (_dataGzipIStream != NULL) delete _dataGzipIStream;
    if (_dataFIStream != NULL) delete _dataFIStream;
    
    // free write path streams
    if (_fileOStream != NULL) delete _fileOStream;
    if (_oStream != NULL) delete _oStream;
    if (_dataOStream != NULL) delete _dataOStream;
    if (_indexOStream != NULL) delete _indexOStream;
    if (_dataGzipOStream != NULL) delete _dataGzipOStream;
    if (_indexOstreamOStream != NULL) delete _indexOstreamOStream;
    if (_indexOstringstream != NULL) delete _indexOstringstream;
}

QBStatus TapFile::open(const string &pipename, const QBioMode &mode)
{
    static const char   *method = "TapFile::open()";
    QBStatus            status;
    string              full_filename = _root_dir + pipename;
    int                 oflags;

    switch(mode) {
        case QB_READ:
        oflags = O_RDONLY;
        break;

        case QB_WRITE:
        oflags = (O_CREAT | O_RDWR | O_TRUNC);
        _create_directory(pipename);
        break;

        case QB_APPEND:
        qb_logerr(QB_FAILURE, __FILE__, __LINE__, method, "Append not yet supported for Tap Files", mode);
        return(QB_FAILURE);
        break;
        
        default:
        qb_logerr(QB_FAILURE, __FILE__, __LINE__, method, "Bad mode: %d", mode);
        return(QB_FAILURE);
    }

    _fd = ::open(full_filename.c_str(), oflags, 0777);
    if (_fd < 0) {
        if (errno == ENOENT) status = QB_FILENOTFOUND;
        else status = QB_FAILURE;
        qb_logerr(status, __FILE__, __LINE__, method,
                  "Error opening for File: %s, errno:%d", pipename.c_str(), errno);
        return(status);
    }
        
    _name       = pipename;
    _mode       = mode;
                            
    switch(_mode) {
        
        // get a couple more file handles
            
        case QB_READ:
            _metaFd = ::open(full_filename.c_str(), oflags, 0777);
            if (_metaFd < 0) {
                if (errno == ENOENT) status = QB_FILENOTFOUND;
                else status = QB_FAILURE;
                qb_logerr(status, __FILE__, __LINE__, method,
                          "Error opening for File:%s, errno:%d", pipename.c_str(), errno);
                return(status);
            }
            _indexFd = ::open(full_filename.c_str(), oflags, 0777);
            if (_indexFd < 0) {
                if (errno == ENOENT) status = QB_FILENOTFOUND;
                else status = QB_FAILURE;
                qb_logerr(status, __FILE__, __LINE__, method,
                          "Error opening for File:%s, errno:%d", pipename.c_str(), errno);
                return(status);
            }            
            
            _read_trailer();
            break;

        case QB_WRITE:   
            _first_write = true;
            _header.Clear();
            _header.set_initial_pipe_name(pipename);
            _header.set_key_descriptor("<NEEDS CODING>");
            _fileOStream = new google::protobuf::io::FileOutputStream(_fd);
            // header gets written on first write
            break;
    }

    return QB_SUCCESS;
}

void TapFile::_read_trailer()
{
    int result;
    uint64_t signature[2];
    string s;
    int sigsize = 2 * sizeof(uint64_t);
    
    //
    // read and confirm signature
    //
    
    result = ::lseek(_metaFd, -sigsize, SEEK_END);
    if (result < 0) throw new TapException("_read_trailer signature seek failed", errno, __FILE__, __LINE__);
    result = ::read(_metaFd, signature, sigsize); 
    if (result < 0) throw new TapException("_read_trailer signature read failed", errno, __FILE__, __LINE__);
    if (memcmp(&(signature[1]), "tapproto", sizeof(uint64_t)) != 0) 
        throw new TapException("_read_trailer found bad signature - file is not a Tap protobuf file", errno, __FILE__, __LINE__);
    
    //
    // read trailer
    //
    
    result = ::lseek(_metaFd, signature[0], SEEK_SET);
    if (result < 0) throw new TapException("_read_trailer trailer seek failed", errno, __FILE__, __LINE__);
    
    _metaFIStream = new google::protobuf::io::FileInputStream(_metaFd);
    _metaIStream = new google::protobuf::io::CodedInputStream(_metaFIStream);
    _metaIStream->ReadString(&s,8);
    
    // confirm the trailer begins with the 'trai' with compression 'none'
    if (s != "trainone") throw new TapException("_read_trailer found invalid trailer signature", errno, __FILE__, __LINE__);
    
    // now deserialize the trailer 
    uint32_t bytes;
    _metaIStream->ReadVarint32(&bytes);
    int limit = _metaIStream->PushLimit(bytes);
    if (!_trailer.ParseFromCodedStream(_metaIStream)) {
        throw new TapException("_read_trailer cant deserialize trailer protobuf", errno, __FILE__, __LINE__);
    }
    _metaIStream->PopLimit(limit);

    // should read the metadata too, do it when our api supports that
    delete _metaIStream;
    delete _metaFIStream;
    
    cout << "Trailer is: " << _trailer.ShortDebugString() << endl;
    
    //
    // prepare for index reads...
    //
    
    result = ::lseek(_metaFd, _trailer.index_offset(), SEEK_SET);
    if (result < 0) throw new TapException("_read_trailer index seek failed", errno, __FILE__, __LINE__);
    _metaFIStream = new google::protobuf::io::FileInputStream(_metaFd);
    _metaIStream = new google::protobuf::io::CodedInputStream(_metaFIStream);
    _metaIStream->ReadString(&s,8);

    // confirm the index begins with the 'upix' with compression 'none'
    if (s != "upixnone") throw new TapException("_read_trailer found invalid index signature", errno, __FILE__, __LINE__);
    
    // ready for action
    _data_block_count = _trailer.data_block_count();
}

QBStatus TapFile::read(QBKey &key, QBEvent &event)
{
    // see if we need to open the next data block
        
    if (_dataIStream == NULL) {
        
        // end of the file?
        if (_data_block_count == 0) return QB_EOF;
        _data_block_count--;
        // read an entry from the index
        uint32_t bytes;
        _metaIStream->ReadVarint32(&bytes);
        int limit = _metaIStream->PushLimit(bytes);
        if (!_index_entry.ParseFromCodedStream(_metaIStream))
            throw new TapException("read() error parsing index entry", errno, __FILE__, __LINE__);
        _metaIStream->PopLimit(limit);
        
        //cout << "index entry " << _index_entry.ShortDebugString() << endl;
        
        //
        // prepare to read the data block
        //
        
        // seek to the data block header
        int result = ::lseek(_fd, _index_entry.data_offset(), SEEK_SET);
        if (result < 0) throw new TapException("read() data seek failed", errno, __FILE__, __LINE__);
        // check the signature is 'data' with compressor 'gzip'
        char sig[9];
        result = ::read(_fd, sig, 8);
        sig[8] = 0;
        if (strcmp(sig,"datagzip") != 0) throw new TapException("read() found invalid data block signature", errno, __FILE__, __LINE__);        
        // setup the CodedInputStream
        _dataFIStream = new google::protobuf::io::FileInputStream(_fd);
        _dataGzipIStream = new google::protobuf::io::GzipInputStream(_dataFIStream);
        _dataIStream = new google::protobuf::io::CodedInputStream(_dataGzipIStream);
        // set the message count
        _message_count = _index_entry.message_count();
        
        //cout << "block count " << _data_block_count << " message count " << _message_count << endl;
    }
    
    // parse the input record
    
    uint32_t bytes; 
    string s;
    _dataIStream->ReadVarint32(&bytes); 
    _dataIStream->ReadString(&s,bytes);
    key.set(s);
    _dataIStream->ReadVarint32(&bytes);
    int lim = _dataIStream->PushLimit(bytes);
    if (!event.ParseFromCodedStream(_dataIStream))
        throw new TapException(("read() error parsing " + _trailer.message_name()).c_str(), errno, __FILE__, __LINE__);
    _dataIStream->PopLimit(lim);
    
    //cout << "read " << bytes << " bytes: " << event.ShortDebugString() << endl;
    
    _message_count--;
    if (_message_count == 0) {
        
        // cleanup the stack o streams
        // dont close the FileInputStream, just delete it, so that it doesnt close the underlying file handle
        
        delete _dataIStream;
        _dataIStream = NULL;
        delete _dataGzipIStream;
        _dataGzipIStream = NULL;
        delete _dataFIStream;
        _dataFIStream = NULL;
    }
    
    return(QB_SUCCESS);
}
    
// write raw bytes to FileOutputStream

void TapFile::_raw(google::protobuf::io::FileOutputStream* fos, const char *raw, int bytes) {
    void *p;
    int transfer, l;
    while (bytes > 0) {
        fos->Next(&p,&l);
        transfer = min(l, bytes);
        memcpy(p, raw, transfer);
        raw += transfer;
        bytes -= transfer;
    }
    fos->BackUp(l-transfer);
}

void TapFile::_raw(google::protobuf::io::FileOutputStream* fos, const char *string) {
    _raw(fos, string, strlen(string));
}
    
// pad to the next sector boundary            
    
static const int Alignment = 512;

void TapFile::_pad(google::protobuf::io::FileOutputStream* fos, int remnant) {
    int padBytes = Alignment - 1 - ((fos->ByteCount() + Alignment-1) % Alignment);
    padBytes -= remnant;
    char pad[Alignment];
    memset(pad, 0xFA, padBytes);
    _raw(fos, pad, padBytes);
}
    
void TapFile::_pad(google::protobuf::io::CodedOutputStream* cos, int remnant) {
    int padBytes = Alignment - 1 - ((cos->ByteCount() + Alignment-1) % Alignment);
    padBytes -= remnant;
    unsigned char pad[Alignment];
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
        _oStream->WriteVarint32(_header.ByteSize());
        _header.SerializeWithCachedSizes(_oStream);
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
        
        _indexOstringstream = new ostringstream(ostringstream::binary|ostringstream::app|ostringstream::out);
        _indexOstreamOStream = new google::protobuf::io::OstreamOutputStream(_indexOstringstream);
        _indexOStream = new google::protobuf::io::CodedOutputStream(_indexOstreamOStream);
        
        _indexOStream->WriteRaw("upix",4);
        _indexOStream->WriteRaw("none",4);
                
        // first-write preprocessing is done
        
        _first_write = false;
    }
    
    // do we have a current open data block?
    
    if (_dataOStream == NULL) {

        // setup the index entry
        
        _index_entry.Clear();
        _index_entry.set_first_key(key.data().c_str(), key.size());
        _index_entry.set_data_offset(_fileOStream->ByteCount());
        _index_entry.set_message_count(0);
        
        // write the data block header 

        _raw(_fileOStream, "data");
        _raw(_fileOStream, "gzip");
        
        // create the GzipOutputStream for the data block
        
        google::protobuf::io::GzipOutputStream::Options options;
        options.compression_level = 6;
        _dataGzipOStream = new google::protobuf::io::GzipOutputStream(_fileOStream, options);
        _dataOStream = new google::protobuf::io::CodedOutputStream(_dataGzipOStream);
    }
    
    // serialize the record
    
    _dataOStream->WriteVarint32(key.size());
    _dataOStream->WriteRaw(key.data().c_str(),key.size());
    _dataOStream->WriteVarint32(event.ByteSize());
    event.SerializeWithCachedSizes(_dataOStream);
        
    _index_entry.set_message_count(_index_entry.message_count() + 1);
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
        _trailer.set_message_count(_trailer.message_count() + _index_entry.message_count());
        _trailer.set_uncompressed_bytes(_trailer.uncompressed_bytes() + 
                                        _dataOStream->ByteCount());
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

        // update the size of this data block in the index
        _index_entry.set_data_bytes(_fileOStream->ByteCount() - _index_entry.data_offset());
                
        //
        // now write out the index entry for this data block
        //
        
        _indexOStream->WriteVarint32(_index_entry.ByteSize());
        _index_entry.SerializeWithCachedSizes(_indexOStream);  
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
        // close out and serialize the whole index
        //
        // yes making an extra copy of the entire index in a string is a bad idea
        // but i had trouble getting to the actual buffer to copy
        //
        // this should probably actually go into a temporary File, and then get copied 
        // over with sendFile(). the data would usually all stay in the buffercache
        //
        
        // delete the coded output stream to flush its contents to the OstreamOutputStrean
        delete _indexOStream;
        _indexOStream = NULL;
        // delete the OstreamOutputStream to flush its contents to the ostringstream
        delete _indexOstreamOStream;
        _indexOstreamOStream = NULL;
        // extract the data 
        string s = _indexOstringstream->str();
        // now delete the ostringstream to free memory
        delete _indexOstringstream;
        _indexOstringstream = NULL;
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
        _oStream->WriteVarint32(_trailer.ByteSize());
        _trailer.SerializeWithCachedSizes(_oStream);
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

bool TapFile::exists(const string &Filename) const
{
    static const char   *method = "TapFile::exists()";

    string full_filename = _root_dir + Filename;

    if (qb_debug()) qb_logmsg("%s: checking existence of %s", method, full_filename.c_str());

    struct stat buf;
    int val = ::stat(full_filename.c_str(), &buf);
    if (val == 0) return(true);
    else return(false);
}

bool TapFile::isOpen() const
{
    if (_fd == 0) return(false);
    else return(true);
}

QBStatus TapFile::_read(string &s) {
    return QB_SUCCESS;
}

QBStatus TapFile::seek(const QBKey &key)
{
    static const char   *method = "TapFile::seek()";
    QBStatus            status;
    string              s;

    if (_mode != QB_READ) {
        status = _flush();
        if (qb_error(status)) {
            qb_logerr(status, __FILE__, __LINE__, method, 
                      "Error flushing File:%s", _name.c_str());
            return(status);
        }
    }

    if (!key.isSet()) {
        if (::lseek(_fd, 0L, SEEK_SET) < 0) {
            qb_logerr(QB_FAILURE, __FILE__, __LINE__, method, 
                      "Error seeking to BOF, File:%s", _name.c_str());
            return(QB_FAILURE);
        }
    }
    else {
        while (_next_key < key) {
            status = _read(s); 
            if (qb_error(status)) { 
                if (status == QB_EOF) return(status); 
                qb_logerr(status, __FILE__, __LINE__, method, "Error reading event"); 
                return(status); 
            } 
             
            status = _read_next_key(); 
            if (qb_error(status)) return(status); 
        }
    }

    return(QB_SUCCESS);
}

QBStatus TapFile::_read_next_key()
{
    const char  *method = "TapFile::_read_next_key()";
    QBStatus    status;
    string      s;

    if (!isOpen()) return(QB_STREAMNOTOPEN);
    if (_mode != QB_READ) return(QB_WRONGACCESSMODE);
    if (_eof_flag) return(QB_EOF);

    // Read the next key.
    status = _read(s); 
    if (qb_error(status)) { 
        if (status == QB_EOF) {
            _eof_flag = true;
            return(QB_SUCCESS);
        }
        else {
            qb_logerr(status, __FILE__, __LINE__, method, "Error reading next key"); 
            return(status); 
        }
    } 
             
    // Deserialize the next key.
    status = _next_key.deserialize(s); 
    if (qb_error(status)) { 
        qb_logerr(status, __FILE__, __LINE__, method, "Error deserializing next key"); 
        return(status); 
    } 

    return(QB_SUCCESS);
}

QBStatus TapFile::nextKey(QBKey &key)
{
    key = _next_key;
    if (_eof_flag) return(QB_EOF);
    return(QB_SUCCESS);
}

QBStatus TapFile::remove(const QBKey &, const QBKey &)
{
    static const char   *method = "TapFile::remove()";
    if (qb_debug()) qb_logmsg("%s - function not available for File operations");
    return(QB_NOTAVAILABLE);
}

QBStatus TapFile::_create_directory(const string &name)
{
    for (int i=1; i < name.size(); i++) {
        if (name[i] == '/') {
            string dir_name = _root_dir + name.substr(0, i);
            struct stat buf;
            if (::stat(dir_name.c_str(), &buf) != 0) {
                if (qb_debug()) qb_logmsg("Creating dir:%s\n", dir_name.c_str());
                mkdir(dir_name.c_str(), 0777);
            }
        }
    }

    return(QB_SUCCESS);
}

}// end namespace