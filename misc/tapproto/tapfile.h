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
        tap::file::UpperIndexEntry  _upper_index_entry;
        tap::file::LowerIndexEntry  _lower_index_entry;
        
        bool                        _first_write;
        
        // write path - coded output streams that we actually write to 
        google::protobuf::io::FileOutputStream*     _fileOStream;
        google::protobuf::io::CodedOutputStream*    _oStream;       
        google::protobuf::io::CodedOutputStream*    _dataOStream;
        google::protobuf::io::CodedOutputStream*    _lowerIxOStream;
        google::protobuf::io::CodedOutputStream*    _upperIxOStream;
        
        // write path - underlying streams that we keep around just to clean up 
        google::protobuf::io::GzipOutputStream*     _dataGzipOStream;
        google::protobuf::io::OstreamOutputStream*  _lowerIxOstreamOStream;
        google::protobuf::io::OstreamOutputStream*  _upperIxOstreamOStream;
        std::ostringstream*                         _lowerIxOstringstream;
        std::ostringstream*                         _upperIxOstringstream;

        // internal state
        int                     _fd;
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
        void                _pad(google::protobuf::io::FileOutputStream* fos, int remnant);
        QBStatus            _read_next_key();
        QBStatus            _create_directory(const string &);
    };
}

#endif
