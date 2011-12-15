//
//  Copyright (c) 2011 Quantbench Corporation, All Rights Reserved.
//

#ifndef _tapfile_
#define _tapfile_

#include "QBio.h"
#include "tapfile.pb.h"
#include <iostream>
#include <sstream>

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/gzip_stream.h>

namespace tap {
    class TapFile : public QBio {
    private:
        tap::file::Header           _header;
        tap::file::Header           _trailer;
        tap::file::IndexEntry       _index_entry;
        
        bool                        _first_write;
 
        // read path
        // coded input streams we actually read from        
        google::protobuf::io::CodedInputStream*     _metaIStream;
        google::protobuf::io::CodedInputStream*     _dataIStream;
        // underlying streams we keep around just to clean up
        google::protobuf::io::FileInputStream*      _metaFIStream;
        google::protobuf::io::GzipInputStream*      _dataGzipIStream;
        google::protobuf::io::FileInputStream*      _dataFIStream;
        
        // write path
        // coded output streams that we actually write to 
        google::protobuf::io::FileOutputStream*     _fileOStream;
        google::protobuf::io::CodedOutputStream*    _oStream;       
        google::protobuf::io::CodedOutputStream*    _dataOStream;
        google::protobuf::io::CodedOutputStream*    _indexOStream;
        // underlying streams that we keep around just to clean up 
        google::protobuf::io::GzipOutputStream*     _dataGzipOStream;
        google::protobuf::io::OstreamOutputStream*  _indexOstreamOStream;
        std::ostringstream*                         _indexOstringstream;

        // state
        int                     _fd;
        int                     _metaFd;
        int                     _indexFd;
        int                     _message_count;
        int                     _data_block_count;
        
        int32_t                 _target_block_size;
        bool                    _eof_flag;
        QBKey                   _next_key;

    public:
        TapFile();
        ~TapFile();

        QBStatus            open(const string &, const QBioMode &);
        QBStatus            read(QBKey &, QBEvent &);
        QBStatus            write(const QBKey &, const QBEvent &);
        QBStatus            seek(const QBKey &);
        QBStatus            remove(const QBKey &start, const QBKey &end);
        QBStatus            nextKey(QBKey &);
        QBStatus            close();
        bool                exists(const string &) const;
        bool                isOpen() const;

    protected:
        QBStatus            _flush();
        QBStatus            _read(string& s);
        void                _read_trailer();
        void                _raw(google::protobuf::io::FileOutputStream* fos, const char *raw, int bytes);
        void                _raw(google::protobuf::io::FileOutputStream* fos, const char *string);
        void                _pad(google::protobuf::io::FileOutputStream* fos, int remnant);
        void                _pad(google::protobuf::io::CodedOutputStream* cos, int remnant);
        QBStatus            _read_next_key();
        QBStatus            _create_directory(const string &);
    };
}

#endif
