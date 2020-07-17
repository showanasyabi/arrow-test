// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <iostream>
#include <chrono>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <string>
#include <cstdint>

#include <vector>


#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/testing/random.h>
#include <arrow/type.h>


using arrow::DoubleBuilder;
using arrow::Int64Builder;
using arrow::ListBuilder;
namespace BitUtil = arrow::BitUtil;
using arrow::internal::BitmapReader;
using arrow::Int8Builder;
using arrow::Status;




void vectorized_chunked_arrow( const std::shared_ptr<arrow::Table>& table){

auto t1 = std::chrono::high_resolution_clock::now();

int16_t male_age = 0 , female_age = 0 , unknown_age = 0;
double result[4]={0,0,0,0};
int counter =0 ;
//std::cout<< *table->schema() << std::endl;
arrow::ChunkedArray* c0p = table->column(0).get();
short  tmp=0, tmp1=0, tmp2=0, tmp3=0,tmp4=0, tmp5=0, tmp6=0,tmp7=0;
std::string str, str1, str2,str3,str4,str5,str6,str7;
arrow::MemoryPool* pool = arrow::default_memory_pool(); 
for (int j= 0; j < table->column(0)->num_chunks(); j++)
{
 auto sex =
    std::static_pointer_cast<arrow::StringArray>(table->column(11)->chunk(j)); // sex
 Int8Builder id_builder(pool);
const int64_t length = c0p->chunk(j)->length();
const int64_t length_rounded = BitUtil::RoundDown(c0p->chunk(j)->length() , 8);
for (int i=0; i< length_rounded; i+=8)
{
    str= sex->GetString(i);
    str1= sex->GetString(i+1); 
    str2= sex->GetString(i+2); 
    str3= sex->GetString(i+3); 
    str4= sex->GetString(i+4); 
    str5= sex->GetString(i+5); 
    str6= sex->GetString(i+6); 
    str7= sex->GetString(i+7); 

    tmp= (  str == "Male") * 1+ (str == "Female") * 2;
    tmp1= (  str1 == "Male") * 1+ (str1 == "Female") * 2;
    tmp2= (  str2 == "Male") * 1+ (str2 == "Female") * 2;
    tmp3= (  str3 == "Male") * 1+ (str3 == "Female") * 2;
    tmp4= (  str4 == "Male") * 1+ (str4 == "Female") * 2;
    tmp5= (  str5 == "Male") * 1+ (str5 == "Female") * 2;
    tmp6= (  str6 == "Male") * 1+ (str6 == "Female") * 2;
    tmp7= (  str7 == "Male") * 1+ (str7 == "Female") * 2 ;
    
    id_builder.Append(tmp);
    id_builder.Append(tmp1);
    id_builder.Append(tmp2);
    id_builder.Append(tmp3);
    id_builder.Append(tmp4);
    id_builder.Append(tmp5);
    id_builder.Append(tmp6);
    id_builder.Append(tmp7);
}
for (int64_t i = length_rounded; i < length; ++i) {
    
	str= sex->GetString(i);
	tmp= (  str == "Male") * 1+ (str == "Female") * 2;
	id_builder.Append(tmp);
	//counter++;
}
std::shared_ptr<arrow::Array> id_array;
id_builder.Finish(&id_array);
//}
 auto ids =
      std::static_pointer_cast<arrow::Int8Array>(id_array);
auto ages =
    std::static_pointer_cast<arrow::DoubleArray>(table->column(12)->chunk(j)); // age
 
   const auto age_values = ages->raw_values();
for (int i=0; i< length_rounded; i+=8){
	result[ids->Value(i)]+=age_values[i];
	result[ids->Value(i+1)]+=age_values[i+1];
	result[ids->Value(i+2)]+=age_values[i+2];
	result[ids->Value(i+3)]+=age_values[i+3];
	result[ids->Value(i+4)]+=age_values[i+4];
	result[ids->Value(i+5)]+=age_values[i+5];
	result[ids->Value(i+6)]+=age_values[i+6];
        result[ids->Value(i+7)]+=age_values[i+7];
}
for (int64_t i = length_rounded; i < length; ++i) {
	result[ids->Value(i)]+=age_values[i];
}
}
auto t2 = std::chrono::high_resolution_clock::now();
auto duration = std::chrono::duration_cast<std::chrono::microseconds>( t2 - t1 ).count();
std::cout << "exe time: of Vectorized chunked arrow:  " << duration << std::endl;
std::cout<< " male: " << result[1] << std::endl;
std::cout<< "female: " << result[2] << std::endl;
std::cout<< " unknown: " << result[0] << std::endl;
}


void  vectorized_arrow ( const std::shared_ptr<arrow::Table>& table){

auto t1 = std::chrono::high_resolution_clock::now();
double  male_age = 0 , female_age = 0 , unknown_age = 0;
arrow::ChunkedArray* c0p = table->column(0).get();
for (int j= 0; j < table->column(0)->num_chunks(); j++){
auto sex =
    std::static_pointer_cast<arrow::StringArray>(table->column(11)->chunk(j)); // sex
auto ages =
    std::static_pointer_cast<arrow::DoubleArray>(table->column(12)->chunk(j)); // age
 
   const auto age_values = ages->raw_values();
for (int i=0; i< c0p->chunk(j)->length(); i++)
{
    if (sex->GetString(i) == "Male")
       male_age +=age_values[i];
    else if (sex->GetString(i) == "Female")
        female_age+=age_values[i];
    else 
       unknown_age += age_values[i];
}
}
auto t2 = std::chrono::high_resolution_clock::now();
auto duration = std::chrono::duration_cast<std::chrono::microseconds>( t2 - t1 ).count();
std::cout << "exe time of vectorized  arrow :" << duration << std::endl;
std::cout<< " male: " << male_age << std::endl;
std::cout<< "female: " << female_age << std::endl;
std::cout<< " unknown: " << unknown_age << std::endl;
//std::cout<< counter << std::endl;

}





Status Readfile(int argc, char** argv) {
  const char* csv_filename = "test.csv";
  const char* arrow_filename = "test.arrow";

  std::cerr << "* Reading CSV file '" << csv_filename << "' into table" << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto input_file,
                        arrow::io::ReadableFile::Open(csv_filename));
  ARROW_ASSIGN_OR_RAISE(
      auto csv_reader,
      arrow::csv::TableReader::Make(arrow::default_memory_pool(),
                                    input_file,
                                    arrow::csv::ReadOptions::Defaults(),
                                    arrow::csv::ParseOptions::Defaults(),
                                    arrow::csv::ConvertOptions::Defaults()));
  ARROW_ASSIGN_OR_RAISE(auto table, csv_reader->Read());
// execute the query for chunked approach
vectorized_chunked_arrow(table);
// execute the query for vectorized approach
vectorized_arrow(table);
  return Status::OK();
}


int main(int argc, char** argv) {
  Status st = Readfile(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
