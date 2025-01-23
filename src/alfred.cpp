// g++ alfredbuttler.cpp misc2.cpp -luuid -lfmt -laws-cpp-sdk-s3 -laws-cpp-sdk-core -lredis++ -lhiredis -lparquet -larrow -DBOOST_LOG_DYN_LINK -lboost_thread -lboost_log -lboost_log_setup -lpthread -lSimpleAmqpClient -lloki-cpp -std=c++2a
// libs = `pkg_config_path=/usr/local/lib/pkgconfig: pkg-config --libs const short = require('short-uuid');uuid
// fmt`
// There was an error, that I solved. I showed they way how I showed below
//https://github.com/aws/aws-sdk-cpp/issues/1920
//for the class I need to try:https://github.com/Paradigm4/bridge/blob/a02dbc7a44ced464f1c3cf6695e7cb44d7ded2ca/src/S3Driver.cpp#L432 
//I need to check: https://github.com/aws/aws-sdk-cpp/issues/1716  
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <tuple>
#include <thread>
#include <vector>
#include <sstream>
#include <iomanip>
#include <cfloat>


#include <sys/stat.h>
#include <unistd.h>


#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/utils/logging/LogLevel.h>

#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <sw/redis++/redis++.h>


#include <SimpleAmqpClient/SimpleAmqpClient.h>

/* gtp: loki
#include <loki/builder.hpp>
 */

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/api.h>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>



//#include "misc.hpp"

unsigned long long my_time = 0;
using json = nlohmann::json;

/* gtp: loki
using namespace loki;
 */
//"tcp://127.0.0.1:6379/1"
//
//
std::string get_env_var(std::string const &key, std::string const &default_value="") {
        const char *val = std::getenv(key.c_str());
        return val == nullptr ? std::string(default_value) : std::string(val);
}


std::string REDIS_HOST = get_env_var("REDIS_HOST");
std::string REDIS_PORT = get_env_var("REDIS_PORT");
std::string REDIS_DB_OPT = get_env_var("REDIS_DB_OPT");
std::string REDIS_URL = fmt::format("tcp://{}:{}/{}", REDIS_HOST, REDIS_PORT, REDIS_DB_OPT);
//"192.168.1.31:3100"

/* gtp: loki
std::string LOKI_HOST = get_env_var("LOKI_HOST");
std::string LOKI_PORT = get_env_var("LOKI_PORT");
std::string LOKI_URL = fmt::format("{}:{}",LOKI_HOST, LOKI_PORT);
 */

auto redis = sw::redis::Redis(REDIS_URL);

constexpr auto EXCHANGE_NAME = "opt4cast_exchange";

std::string AMQP_HOST = get_env_var("AMQP_HOST");
std::string AMQP_USERNAME = get_env_var("AMQP_USERNAME");
std::string AMQP_PASSWORD = get_env_var("AMQP_PASSWORD");
std::string AMQP_PORT = get_env_var("AMQP_PORT");
const std::string OPT4CAST_WAIT_MILLISECS_IN_CAST = get_env_var("OPT4CAST_WAIT_MILLISECS_IN_CAST");

std::string msu_cbpo_path = get_env_var("MSU_CBPO_PATH", "/opt/opt4cast");

// Create a registry
//.Remote("127.0.0.1:3000")
//.Remote("192.168.1.21:3100/loki/api/v1/push")

/* gtp: loki
auto registry = Builder<AgentJson>{}
                    .Remote(LOKI_URL)
                    .LogLevel(Level::Debug)
                    .PrintLevel(Level::Debug)
                    .Colorize(Level::Warn,  Color::Yellow)
                    .Colorize(Level::Error, Color::Red)
                    .Colorize(Level::Info, Color::Green)
                    .FlushInterval(10)
                    .MaxBuffer(10)
                    .Labels({
                      {"namespace", "production"},
                      {"location", "en"}
                    })
                    .Build();


auto &logger = registry.Add({{"job", "opt4cast"}});
 */ //gtp: end loki
unsigned long long get_time() {
    namespace sc = std::chrono;
    auto time = sc::system_clock::now();

    auto since_epoch = time.time_since_epoch();
    auto millis = sc::duration_cast<sc::milliseconds>(since_epoch);

    unsigned long long now = millis.count(); // just like java (new Date()).getTime();
    return now;
}

std::string cout_time() {
    std::time_t t2 = time(0);
    std::stringstream strm;
    strm << std::put_time(std::gmtime(&t2), "%F %X");
    return fmt::format("{}", strm.str());
}

namespace BareParquet {
    // #1 Write out the data as a Parquet file
    void write_csv_file(const arrow::Table &table, const std::string csv_filename) {
        PARQUET_ASSIGN_OR_THROW(auto outstream, arrow::io::FileOutputStream::Open(csv_filename));
        PARQUET_THROW_NOT_OK(arrow::csv::WriteCSV(table, arrow::csv::WriteOptions::Defaults(), outstream.get()));
    }

    void parquet_to_csv(std::string parquet_filename, std::string csv_filename) {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(parquet_filename, arrow::default_memory_pool()));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
        write_csv_file(*table, csv_filename);
    }
}
void split_str(std::string const &str, const char delim,
               std::vector<std::string> &out) {
    std::stringstream s(str);

    std::string s2;

    while (std::getline(s, s2, delim)) {
        out.push_back(s2); // store the string in s2
    }
}
/*
int erase_file_line(std::string path) {
    std::string line;
    std::ifstream fin;

    fin.open(path);
    std::ofstream temp;
    temp.open("temp.txt");

    bool flag = true;
    int val = -1;
    while (getline(fin, line)) {
        if (flag == true) {
            val = std::stoi(line);
            flag = false;
        } else {
            temp << line << std::endl;
        }
    }

    temp.close();
    fin.close();

    // required conversion for remove and rename functions
    const char *p = path.c_str();
    remove(p);
    rename("temp.txt", p);
    return val;
}
*/

namespace awss3 {
    bool put_object(const Aws::String &bucket_name,
                    const Aws::String &object_name,
                    const Aws::String &local_object_name,
                    const Aws::String &region = "us-east-1") {
        struct stat buffer;
        bool ret;

        if (stat(local_object_name.c_str(), &buffer) == -1) {
            std::string cerror = fmt::format("[S3] [LOCAL_FILE] [DOESNT_EXIST] [{}]", local_object_name);

            /* gtp: loki
            logger.Infof(fmt::format("{} {}", cout_time(), clog));
             */
            //Aws::ShutdownAPI(options);
            return false;
        }

        Aws::Client::ClientConfiguration config;

        if (!region.empty()) {
            config.region = region;
        }

        Aws::S3::S3Client s3_client(config);
        //s3_client = std::make_unique<Aws::S3::S3Client>(config);
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);

        std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
                "sampleallocationtag", local_object_name.c_str(),
                std::ios_base::in | std::ios_base::binary);

        request.SetBody(input_data);

        Aws::S3::Model::PutObjectOutcome outcome = s3_client.PutObject(request);

        if (outcome.IsSuccess()) {
            ret = true;
        } else {

            unsigned long long my_new_time = (get_time() - my_time) / 1000;
            std::string cerror = fmt::format("[S3] [PUT_OBJECT] {} {} {} {} {}", outcome.GetError().GetMessage(), my_new_time, bucket_name, object_name, local_object_name);
            /* gtp: loki
            logger.Infof(fmt::format("{} {}", cout_time(), clog));
             */
            ret = false;
        }
        return ret;
    }

    bool put_object_buffer(const Aws::String &bucket_name,
                           const Aws::String &object_name,
                           const std::string &object_content,
                           const Aws::String &region = "us-east-1") {

        bool ret = true;
        Aws::Client::ClientConfiguration config;

        if (!region.empty()) {
            config.region = region;
        }

        Aws::S3::S3Client s3_client(config);

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);

        const std::shared_ptr<Aws::IOStream> input_data =
                Aws::MakeShared<Aws::StringStream>("");
        *input_data << object_content.c_str();

        request.SetBody(input_data);

        Aws::S3::Model::PutObjectOutcome outcome = s3_client.PutObject(request);

        if (!outcome.IsSuccess()) {
            std::string cerror= fmt::format("[S3] [PUT_OBJECT] {}", outcome.GetError().GetMessage());

            /* gtp: loki
            logger.Infof(fmt::format("{} {}", cout_time(), clog));
             */

            ret = false;;
        } else {
            ret = true;
        }

        return ret;
    }

// write to file
// https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/configuring-iostreams.html
// https://stackoverflow.com/questions/64688716/is-aws-GetObject-downloading-all-the-data-into-memory-or-to-a-file-cache

    bool get_object(const Aws::String &bucket_name,
                    const Aws::String &object_name,
                    const std::string &local_object_name,
                    const Aws::String region = "us-east-1") {

        bool ret = true;
        Aws::Client::ClientConfiguration config;
        if (!region.empty()) {
            config.region = region;
        }

        Aws::S3::S3Client s3_client(config);

        Aws::S3::Model::GetObjectRequest object_request;

        object_request.SetBucket(bucket_name);
        object_request.SetKey(object_name);
        object_request.SetResponseStreamFactory([=]() {
            // return aws::new<aws::fstream>("s3download", hthis->m_origfilename,
            // std::ios_base::out | std::ios_base::binary);
            return Aws::New<Aws::FStream>("s3download", local_object_name,
                                          std::ios_base::out | std::ios_base::binary);
        });
        // auto get_object_outcome = s3_client.get()->GetObject(object_request);
        Aws::S3::Model::GetObjectOutcome get_object_outcome =
                s3_client.GetObject(object_request);

        if (get_object_outcome.IsSuccess()) {
            ret = true;
        } else {
            auto err = get_object_outcome.GetError();

            int my_new_time = (get_time() - my_time) / 1000;
            std::string cerror = fmt::format("[S3] [GET_OBJECT] {} {} MS:{} {} {} {}", err.GetExceptionName(), err.GetMessage(), my_new_time, bucket_name, object_name, local_object_name);

            /* gtp: loki
            logger.Infof(fmt::format("{} {}", cout_time(), clog));
             */

            ret = false;
        }
        return ret;
    }

    bool is_object(const Aws::String &bucket_name,
                   const Aws::String &object_name,
                   const Aws::String region = "us-east-1") {
        bool ret = false;
        Aws::Client::ClientConfiguration config;
        if (!region.empty()) {
            config.region = region;
        }

        Aws::S3::S3Client s3_client(config);

        Aws::S3::Model::GetObjectRequest object_request;

        object_request.SetBucket(bucket_name);
        object_request.SetKey(object_name);

        Aws::S3::Model::GetObjectOutcome get_object_outcome =
                s3_client.GetObject(object_request);

        if (get_object_outcome.IsSuccess()) {
            ret = true;
        } else {
            /*
            auto err = get_object_outcome.GetError();

            std::string clog = fmt::format("[S3] [IS_OBJECT] {}", err.GetExceptionName(), err.GetMessage() );
            auto cerror =  clog;
            logger.Infof(fmt::format("{} {}", cout_time(), clog));
            */
            ret = false;
        }
        return ret;
    }

    bool delete_object(const Aws::String &from_bucket,
                       const Aws::String &object_key) {
        bool ret = true;
        Aws::String region = "us-east-1";

        Aws::Client::ClientConfiguration clientconfig;
        if (!region.empty())
            clientconfig.region = region;

        Aws::S3::S3Client client(clientconfig);
        Aws::S3::Model::DeleteObjectRequest request;

        request.WithKey(object_key).WithBucket(from_bucket);

        Aws::S3::Model::DeleteObjectOutcome outcome = client.DeleteObject(request);

        if (!outcome.IsSuccess()) {
            // it is not nec
            //auto err = outcome.GetError();
            //std::string clog = fmt::format("[S3] [DELETE_OBJECT] {}", err.GetExceptionName(), err.GetMessage() );
            //auto cerror =  clog;
            //logger.Infof(fmt::format("{} {}", cout_time(), clog));
            ret = false;
        } else {
            ret = true;
        }
        return ret;
    }

} // namespace awss3
/*
void doguidstuff() {
  auto myguid = xg::newguid();
  std::stringstream stream;
  stream << myguid;
  auto guidstring = stream.str();
}
*/

bool is_really_finished(std::string filename) {
    try {
        if (awss3::is_object("cast-optimization-dev", filename) == true) {
            return true;
        }

    } catch (int e) {
        std::cout << "error: " << e << std::endl;
    }
    return false;
}

bool wait_for_file(std::string filename) {
    bool ret = false;
    int counter = 0;
    while (true) {
        if (is_really_finished(filename) == false) {
            std::cout << "File has not finished: " << std::endl;
            if (counter > 100)
                break;

            int seconds = 1;
            sleep(seconds);
            //std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
            counter++;
        } else {
            ret = true;
            break;
        }
    }

    return ret;
}


bool delete_modeloutput_files(std::string scenario_id) {
    std::string bucket_name = "cast-optimization-dev";
    std::string filename;

    //modeloutput
    filename = fmt::format("data/scenarios/modeloutput/animalfractioninlrseg/scenarioid={}/animalfractioninlrseg.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/bmpcreditedanimal/scenarioid={}/bmpcreditedanimal.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/bmpcreditedland/scenarioid={}/bmpcreditedland.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/bmpcreditedmanuretransport/scenarioid={}/bmpcreditedmanuretransport.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/cropapplicationmassmonthly/scenarioid={}/cropapplicationmassmonthly.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/cropcoverresults/scenarioid={}/cropcoverresults.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/croplanduse/scenarioid={}/croplanduse.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/cropnitrogenfixationresults/scenarioid={}/cropnitrogenfixationresults.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/cropnutrientsapplied/scenarioid={}/cropnutrientsapplied.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/landusepostbmp/scenarioid={}/landusepostbmp.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/loadseor/scenarioid={}/loadseor.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/loadseos/scenarioid={}/loadseos.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/loadseot/scenarioid={}/loadseot.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/loadssallbsperacre/scenarioid={}/loadssallbsperacre.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/manurenutrientsconfinement/scenarioid={}/manurenutrientsconfinement.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/manurenutrientsdirectdeposit/scenarioid={}/manurenutrientsdirectdeposit.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/manurenutrientstransported/scenarioid={}/manurenutrientstransported.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/reportloads/scenarioid={}/reportloads.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/septicloads/scenarioid={}/septicloads.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/modeloutput/septicsystemspostbmp/scenarioid={}/septicsystemspostbmp.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    return true;
}

bool delete_metadata_files(std::string scenario_id) {
    std::string bucket_name = "cast-optimization-dev";
    std::string filename;
    //metadata
    filename = fmt::format("data/scenarios/metadata/impbmpsubmittedanimal/scenarioid={}/impbmpsubmittedanimal.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/metadata/impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/metadata/impbmpsubmittedmanuretransport/scenarioid={}/impbmpsubmittedmanuretransport.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/metadata/impbmpsubmittedrelatedmeasure/scenarioid={}/impbmpsubmittedrelatedmeasure.parquet", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/metadata/scenario/scenarioid={}/scenario.json", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/metadata/scenariogeography/scenarioid={}/scenariogeography.json", scenario_id);
    awss3::delete_object(bucket_name, filename);
    return true;
}

bool delete_indexes_files(std::string scenario_id) {
    std::string bucket_name = "cast-optimization-dev";
    std::string filename;
    //indexes
    filename = fmt::format("data/scenarios/indexes/scenarioid={}/landuse/BmpToLandUseIndexes.index", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/indexes/scenarioid={}/landuse/LandUseIndexes.index", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/indexes/scenarioid={}/lrsegfraction/BmpToLandRiverFraction.index", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/indexes/scenarioid={}/lrsegfraction/LandRiverFractionIndexes.index", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/indexes/scenarioid={}/septicuse/BmpToSepticUseIndexes.index", scenario_id);
    awss3::delete_object(bucket_name, filename);
    filename = fmt::format("data/scenarios/indexes/scenarioid={}/streamuse/BmpToStreamShoreUseIndexes.index", scenario_id);
    awss3::delete_object(bucket_name, filename);
    return true;
}

bool delete_files(std::string scenario_id) {
    delete_modeloutput_files(scenario_id);
    delete_indexes_files(scenario_id); 
    delete_metadata_files(scenario_id);
    return true;
}


std::tuple<json, json, json> create_jsons(std::string scenario_id, std::string exec_uuid) {

    std::string emo_data_str = *redis.hget("emo_data", exec_uuid);
    std::vector<std::string> emo_data_list;
    split_str(emo_data_str, '_', emo_data_list);

    std::string scenario_name = emo_data_list[0];
    int atm_dep_data_set_id = std::stoi(emo_data_list[1]);
    int back_out_scenario_id = std::stoi(emo_data_list[2]);
    int base_condition_id = std::stoi(emo_data_list[3]);
    int base_load_id = std::stoi(emo_data_list[4]);
    int cost_profile_id = std::stoi(emo_data_list[5]);
    int climate_change_data_set_id = std::stoi(emo_data_list[6]);
    int geography_nids = std::stoi(emo_data_list[7]);
    int historical_crop_need_scenario_id = std::stoi(emo_data_list[8]);
    int point_source_data_set_id = std::stoi(emo_data_list[9]);
    int scenario_type_id = std::stoi(emo_data_list[10]);
    int soil_p_data_set_id = std::stoi(emo_data_list[11]);
    int source_data_revision_id = std::stoi(emo_data_list[12]);

    std::vector<int> geography_id_list;
    int geography_tmp;
    fmt::print("geography_nids: {}\n", geography_nids);
    for (int i(0); i < geography_nids; ++i) {
        fmt::print("{}\n", emo_data_list[13 + i]);
        
        geography_tmp = std::stoi(emo_data_list[13 + i]);
        geography_id_list.push_back(geography_tmp);
    }

    json scenario;
    int scenario_id_int = std::stoi(scenario_id);

    scenario["ScenarioId"] = scenario_id_int;
    scenario["ScenarioName"] = scenario_name;
    scenario["BaseConditionId"] = base_condition_id;
    scenario["BackOutScenarioId"] = back_out_scenario_id;
    scenario["ScenarioTypeId"] = scenario_type_id;
    scenario["CostProfileId"] = cost_profile_id;
    scenario["SoilPDataSetId"] = soil_p_data_set_id;
    scenario["HistoricalCropNeedScenarioId"] = historical_crop_need_scenario_id;
    scenario["PointSourceDataSetId"] = point_source_data_set_id;
    scenario["AtmDepDataSetId"] = atm_dep_data_set_id;
    scenario["ClimateChangeDataSetId"] = climate_change_data_set_id;
    scenario["BaseLoadId"] = base_load_id;
    scenario["SourceDataRevisionId"] = source_data_revision_id;


    json user_data = json::parse(
            R"({ "EventText": "", "FirstName": "", "LastName": "", "Notify": [""], "EventKey": "RunScenario", "NotifiedBy": "", "NotifyReason": "", "UserId": "00000000-0000-0000-0000-000000000000" })");
    json data_transfer_object = json::parse(
            R"({ "CopyAg": [], "CopyDc": [], "CopyDe": [], "CopyMd": [], "CopyNy": [], "CopyPenn": [], "CopyVa": [], "CopyWva": [], "CopyNat": [], "CopySept": [], "CopyDev": [], "CopyPol": [], "CopySingle": [], "NeedsReRun": false, "IsPublic": false, "IsOptimizationMode": true, "NeedsValidation": false, "StatusId": 0, "Name": null, "Source": 0, "SourceDataVersion": "", "NetworkPath": "", "LandBmpImportFiles": [], "AimalBmpImportFiles": [], "ManureBmpImportFiles": [], "LandPolicyBmpImportFiles": [] })");
    data_transfer_object["Id"] = scenario_id_int;
    data_transfer_object["FileId"] = exec_uuid;
    data_transfer_object["UserData"] = user_data;
    json core = json::parse(R"({ "ProcessorFactory": "ScenarioProcessorFactory", "ProcessorServiceName": "ScenarioRunProcessor"})");
    core["DataTransferObject"] = data_transfer_object;

    json geography;
    // even easier with structured bindings (c++17)
    for (auto &&geography_id: geography_id_list) {
        json geography_item;
        geography_item["ScenarioId"] = scenario_id_int;
        geography_item["GeographyId"] = geography_id;
        geography.emplace_back(geography_item);
    }


    redis.hset(exec_uuid, "core", core.dump());
    redis.hset(exec_uuid, "scenario", scenario.dump());
    redis.hset(exec_uuid, "geography", geography.dump());

    return std::make_tuple(core, scenario, geography);
}


bool send_json_streams(std::string scenario_id,
                       std::string emo_uuid,
                       std::string exec_uuid,
                       bool del_files = true) {
    std::string core_str;
    std::string scenario_str;
    std::string geography_str;

    if (redis.hexists(emo_uuid, "core") == false) {
        create_jsons(scenario_id, emo_uuid);
    }
    core_str = *redis.hget(emo_uuid, "core");
    scenario_str = *redis.hget(emo_uuid, "scenario");
    geography_str = *redis.hget(emo_uuid, "geography");

    int scenario_id_int = std::stoi(scenario_id);
    json core = json::parse(core_str);
    json scenario = json::parse(scenario_str);
    json geography = json::parse(geography_str);
    core["DataTransferObject"]["Id"] = scenario_id_int;
    core["DataTransferObject"]["FileId"] = exec_uuid;
    scenario["ScenarioId"] = scenario_id_int;

    //std::clog<<core<<std::endl;

    for (auto &&geography_tmp: geography) {
        geography_tmp["ScenarioId"] = scenario_id_int;
    }
    //scenarios:
    std::string scenario_path = fmt::format("data/scenarios/metadata/scenario/scenarioid={}/scenario.json", scenario_id);
    fmt::print("scenario_path: {}\n", scenario_path);
    std::string scenario_geography_path = fmt::format("data/scenarios/metadata/scenariogeography/scenarioid={}/scenariogeography.json", scenario_id);
    fmt::print("scenario_geography_path: {}\n", scenario_geography_path);
    //bmps
    std::string impbmpsubmittedland_path = fmt::format("data/scenarios/metadata/impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet", scenario_id);
    fmt::print("impbmpsubmittedland_path: {}\n", impbmpsubmittedland_path);
    std::string impbmpsubmittedanimal_path = fmt::format("data/scenarios/metadata/impbmpsubmittedanimal/scenarioid={}/impbmpsubmittedanimal.parquet", scenario_id);
    std::string impbmpsubmittedmanuretransport_path = fmt::format("data/scenarios/metadata/impbmpsubmittedmanuretransport/scenarioid={}/impbmpsubmittedmanuretransport.parquet", scenario_id);
    //output
    std::string reportloads_path = fmt::format("data/scenarios/modeloutput/reportloads/scenarioid={}/reportloads.parquet", scenario_id);
    //thrigger

    std::string core_path = fmt::format("lambdarequests/optimize/optimizeSce_{}.json", exec_uuid);
    fmt::print("core_path: {}\n", core_path);

    try {
        if (awss3::put_object_buffer("cast-optimization-dev", scenario_path, scenario.dump()) == false)
            return false;
        wait_for_file(scenario_path);


        if (awss3::put_object_buffer("cast-optimization-dev", scenario_geography_path, geography.dump()) == false)
            return false;

        wait_for_file(scenario_geography_path);

        //delete
        if (del_files == true) {
            awss3::delete_object("cast-optimization-dev", impbmpsubmittedland_path);
            awss3::delete_object("cast-optimization-dev", impbmpsubmittedanimal_path);
            awss3::delete_object("cast-optimization-dev", impbmpsubmittedmanuretransport_path);
        }
        awss3::delete_object("cast-optimization-dev", reportloads_path);
        int seconds = 2;
        std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
        if (awss3::is_object("cast-optimization-dev", reportloads_path) == true)
            return false;


        if (awss3::put_object_buffer("cast-optimization-dev", core_path, core.dump()) == true)
            return true;

    }
    catch (const std::exception &error) {
        auto cerror = fmt::format("S3_ERROR Sending files: {}", error.what());
        std::cout << cerror<<"\n"; 
    }

    return false;
}

bool send_message(std::string routing_name, std::string msg) {
    bool return_val = false;

    AmqpClient::Channel::OpenOpts opts;
    opts.host = AMQP_HOST.c_str();
    opts.port = std::stoi(AMQP_PORT);
    opts.vhost = "/";
    opts.frame_max = 131072;
    opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth(AMQP_USERNAME, AMQP_PASSWORD);
    try {
        auto channel = AmqpClient::Channel::Open(opts);
        channel->DeclareExchange(EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, false, true, false);
        // Enable publisher confirmations for the channel.
        auto message = AmqpClient::BasicMessage::Create(msg);
        channel->BasicPublish(EXCHANGE_NAME, routing_name, message, false, false);
        return_val = true;
    }
    catch (const std::exception &error) {
        auto cerror = fmt::format("error {} ", error.what());
        std::cout << "Error in send Message to RabbitMQ " << error.what() << "\n";
    }

    return return_val;
}


bool send_submitted_land(std::string scenario_id, std::string emo_uuid, std::string exec_uuid) {
    std::string impbmpsubmittedland_path = fmt::format("data/scenarios/metadata/impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet", scenario_id);
    if (awss3::put_object("cast-optimization-dev", impbmpsubmittedland_path,
                          fmt::format("{}/output/nsga3/{}/{}_impbmpsubmittedland.parquet", msu_cbpo_path, emo_uuid, exec_uuid)) == true)
        return true;
    return false;
}

bool send_files(std::string scenario_id, std::string emo_uuid, std::string exec_uuid) {
    auto land_path = fmt::format("data/scenarios/metadata/impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet", scenario_id);
    auto land_filename = fmt::format("{}/output/nsga3/{}/{}_impbmpsubmittedland.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
    auto animal_path = fmt::format("data/scenarios/metadata/impbmpsubmittedanimal/scenarioid={}/impbmpsubmittedanimal.parquet", scenario_id);
    auto animal_filename = fmt::format("{}/output/nsga3/{}/{}_impbmpsubmittedanimal.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
    auto manuretransport_path = fmt::format("data/scenarios/metadata/impbmpsubmittedmanuretransport/scenarioid={}/impbmpsubmittedmanuretransport.parquet", scenario_id);
    auto manuretransport_filename = fmt::format("{}/output/nsga3/{}/{}_impbmpsubmittedmanuretransport.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
    auto file_sent = false;

    awss3::delete_object("cast-optimization-dev", land_path);
    awss3::delete_object("cast-optimization-dev", animal_path);
    awss3::delete_object("cast-optimization-dev", manuretransport_path);

    if (std::filesystem::exists(land_filename) &&
        awss3::put_object("cast-optimization-dev", land_path, land_filename)) {
        file_sent = true;
        fmt::print("land file sent: scenario_id: {}, exec_id: {}\n", scenario_id, exec_uuid);
    }

    if (std::filesystem::exists(animal_filename) &&
        awss3::put_object("cast-optimization-dev", animal_path, animal_filename)) {
        file_sent = true;
    }

    if (std::filesystem::exists(manuretransport_filename) &&
        awss3::put_object("cast-optimization-dev", manuretransport_path, manuretransport_filename)) {
        file_sent = true;
    }

    return file_sent;
}


bool is_finished(std::string scenario_id) {
    std::string reportloads_path = fmt::format("data/scenarios/modeloutput/reportloads/scenarioid={}/reportloads.parquet", scenario_id);
    try {
        if (awss3::is_object("cast-optimization-dev", reportloads_path) == true) {
            return true;
        }

    } catch (int e) {
        std::cout << "error: " << e << std::endl;
    }
    return false;
}

bool wait_for_files(std::string scenario_id) {
    std::string reportloads_path = fmt::format("data/scenarios/modeloutput/reportloads/scenarioid={}/reportloads.parquet", scenario_id);
    std::string path_output = fmt::format(
            "data/scenarios/modeloutput/reportloads/scenarioid={}", scenario_id);
    std::string land_filename = fmt::format(
            "impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet",
            scenario_id);
    std::string report_loads = "reportloads.parquet";

    bool ret = false;
    int counter = 0;
    while (true) {
        if (is_finished(scenario_id) == false) {
            if (counter > 10)
                break;

            int seconds = 2;
            std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
            counter++;
        } else {
            ret = true;
            break;
        }
    }

    return ret;
}

std::vector<double> read_loads(std::string loads_filename) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(loads_filename, arrow::default_memory_pool()));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::ChunkedArray> array;
    arrow::Datum sum;

    std::vector<double> loads;

    for (int col(7); col < 16; ++col) {
        PARQUET_THROW_NOT_OK(reader->ReadColumn(col, &array));
        PARQUET_ASSIGN_OR_THROW(sum, arrow::compute::Sum(array));
        double val = (std::dynamic_pointer_cast<arrow::DoubleScalar>(sum.scalar()))->value;
        loads.emplace_back(val);
    }
    return loads;
}

bool wait_to_download_file(std::string url, std::string filename, int ntries, int wait_nsecs) {
    bool ans = false;
    while (true) {
        ans = awss3::get_object("cast-optimization-dev", url, filename);
        if (ans == true || ntries == 0) {
            break;
        } else {
            --ntries;
            std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(wait_nsecs));
        }
    }
    if (ans == false) {
        auto clog = fmt::format("We could not download file {} from {} after {} seconds",filename, url, wait_nsecs);
        return false;
    }
    return true;
}

bool download_parquet_file(std::string base_url, std::string path, std::string base_filename, std::string prefix_filename, std::string scenario_id) {
    std::string url = fmt::format("data/scenarios/{}/{}/scenarioid={}/{}.parquet", base_url, base_filename, scenario_id, base_filename);

    if (std::filesystem::exists(path) == false) {
        if (std::filesystem::create_directories(path) == false) {
            auto clog = fmt::format("[CREATE_DIRECTORY] {} ", path);
        }
    }
    std::string filename;
    filename = fmt::format("{}/{}{}.parquet", path, prefix_filename, base_filename);

    auto filename_csv = fmt::format("{}/{}{}.csv", path, prefix_filename, base_filename);

    if (awss3::is_object("cast-optimization-dev", url) == false)
        return false;

    wait_to_download_file(url, filename, 3, 1);

    BareParquet::parquet_to_csv(filename, filename_csv);
    return true;
}

bool get_files(std::string scenario_id, std::string emo_uuid, std::string exec_uuid) {
    bool ret = false;;

    if (emo_uuid == exec_uuid) {
        std::string dir_path = fmt::format("{}/output/nsga3/{}/config", msu_cbpo_path, emo_uuid);
        std::string base_filename = "reportloads";
        std::string prefix_filename = "";
        ret = download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        //base_filename = "impbmpsubmittedland";
        //download_parquet_file("metadata", dir_path, base_filename, scenario_id);
        base_filename = "animalfractioninlrseg";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "bmpcreditedland";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "manurenutrientsconfinement";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "manurenutrientsdirectdeposit";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "septicloads";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
    } else {
        std::string dir_path = fmt::format("{}/output/nsga3/{}", msu_cbpo_path, emo_uuid);
        std::string base_filename = "reportloads";
        std::string prefix_filename = fmt::format("{}_", exec_uuid);
        ret = download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
    }
    return ret;
}

bool get_files2(std::string scenario_id, std::string emo_uuid, std::string exec_uuid) {
    bool ret = false;;
    std::string path = "data/scenarios/metadata";

    std::string land_filename = fmt::format(
            "impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet",
            scenario_id);
    std::string animal_filename = fmt::format(
            "impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet",
            scenario_id);
    std::string manure_transport_filename = fmt::format(
            "impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet",
            scenario_id);

    std::string bmp_credited_land = "bmpcreditedland.parquet";
    std::string local_report_loads;
    std::string local_report_loads_csv;

    std::string dir_path = fmt::format("{}/output/nsga3/{}", msu_cbpo_path, emo_uuid);
    if (std::filesystem::exists(dir_path) == false) {
        auto ret = std::filesystem::create_directories(dir_path);
        if (ret == false) {
            auto clog = fmt::format("[CREATE_DIRECTORY] {} ", dir_path);
        }
    }
    if (emo_uuid == exec_uuid) {
        dir_path = fmt::format("{}/output/nsga3/{}/config", msu_cbpo_path, emo_uuid);
        if (std::filesystem::exists(dir_path) == false) {
            auto ret = std::filesystem::create_directories(dir_path);
            if (ret == false) {
                auto clog = fmt::format("[CREATE_DIRECTORY] {} ", dir_path);
            }
        }

        local_report_loads = fmt::format("{}/output/nsga3/{}/config/reportloads.parquet", msu_cbpo_path, emo_uuid);
        local_report_loads_csv = fmt::format("{}/output/nsga3/{}/config/reportloads.csv", msu_cbpo_path, emo_uuid);
    } else {
        local_report_loads = fmt::format("{}/output/nsga3/{}/{}_reportloads.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
        local_report_loads_csv = fmt::format("{}/output/nsga3/{}/{}_reportloads.csv", msu_cbpo_path, emo_uuid, exec_uuid);
    }

    std::string path_output = fmt::format("data/scenarios/modeloutput/reportloads/scenarioid={}", scenario_id);
    std::string report_loads = "reportloads.parquet";
    auto object_key = fmt::format("{}/{}", path_output, report_loads);

    if (awss3::is_object("cast-optimization-dev", object_key) == true) {

        int seconds = 1;
        int try_again = 3;
        bool ans = false;
        while (true) {
            ans = awss3::get_object("cast-optimization-dev", object_key, local_report_loads);
            if (ans == true || try_again == 0) { break; }
            else {
                --try_again;
                std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
            }
        }
        if (ans == false) {
            auto clog = fmt::format("We could not download file {} to {} on time: ", object_key, local_report_loads);
            return false;
        }

        BareParquet::parquet_to_csv(local_report_loads, local_report_loads_csv);
        namespace fs = std::filesystem;
        fs::path f{local_report_loads};
        int counter = 0;
        while (fs::exists(f) == false) {
            if (counter > 3) {
                auto clog =  "We could not download file on time: ";
                return false;
            }
            std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
            counter++;
        }
        ret = true;
    }

    object_key = fmt::format("{}/{}", path, bmp_credited_land);
    if (awss3::is_object("cast-optimization-dev", object_key) == true) {
        awss3::get_object("cast-optimization-dev", object_key,
                          "bmpcreditedland.parquet");
        //ret = true;
    }

    object_key = fmt::format("{}/{}", path, land_filename);
    if (awss3::is_object("cast-optimization-dev", object_key) == true) {
        awss3::get_object("cast-optimization-dev", object_key,
                          "impbmpsubmittedland.parquet");
        //ret = true;
    }
    return ret;
}
//*/


bool execute_20(std::string scenario_id, std::string emo_uuid, std::string exec_uuid) {
    bool got_files = false;
    got_files = get_files(scenario_id, emo_uuid, exec_uuid);
    if (got_files == true) {
        std::string loads_filename;
        if (emo_uuid == exec_uuid) {
            loads_filename = fmt::format("{}/output/nsga3/{}/config/reportloads.parquet", msu_cbpo_path, emo_uuid);
            std::string local_report_loads_csv = fmt::format("{}/output/nsga3/{}/config/reportloads.csv", msu_cbpo_path, emo_uuid);
            auto input_path = fmt::format("{}/input", msu_cbpo_path);
            if (std::filesystem::exists(input_path) == false) {
                auto ret = std::filesystem::create_directories(input_path);
            }
            std::string local_report_loads_csv2 = fmt::format("{}/input/{}_reportloads.csv", msu_cbpo_path, emo_uuid);
            std::filesystem::copy(local_report_loads_csv, local_report_loads_csv2);
        } else {
            loads_filename = fmt::format("{}/output/nsga3/{}/{}_reportloads.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
        }
        auto loads = read_loads(loads_filename);
        redis.hset("executed_results", exec_uuid, fmt::format("{:.2f}_{:.2f}_{:.2f}", loads[0], loads[1], loads[2]));


        //loads_json = json.dump(loads);

        //base_start_time = start_time.pop(exec_uuid, 0)
        //base_end_time  = datetime.datetime.now()
        //base_time_difference = int((base_end_time - base_start_time).total_seconds())
    } else {
        auto clog =  "Error in retrieving files: ";
        std::cout<<clog<<"\n";

        /* gtp: loki
        logger.Infof(fmt::format("{} [Error in retrieving] [{}] [{}]", cout_time(), exec_uuid, scenario_id));
         */
        return false;
        /*
        std::string loads_filename = fmt::format("{}/output/read/{}_reportloads.parquet", msu_cbpo_path, exec_uuid);
        redis.hset("executed_results", exec_uuid, fmt::format("{:.2f}_{:.2f}_{:.2f}", 999999.99, 999999.99, 999999.99));
        redis.rpush("scenario_ids", scenario_id);
        */
    }
    return got_files;
}

void retrieve_exec() {

    std::vector<std::string> to_retrieve_list;
    redis.lrange("retrieving_queue", 0, -1, std::back_inserter(to_retrieve_list));
    int n_to_retrieve = to_retrieve_list.size();
    redis.ltrim("retrieving_queue", n_to_retrieve, -1);
    //redis.del({"retrieving_queue"});


    //std::vector<std::string> added_to_retrieving_queue;
    //redis.lrange("added_to_retrieving_queue", 0, -1, std::back_inserter(added_to_retrieving_queue));
    //redis.del({"added_to_retrieving_queue"});


    if (n_to_retrieve > 0) {
        std::cout << "Retrieving: " << n_to_retrieve << std::endl;
        if (n_to_retrieve == 1) {
            std::string exec_str = *redis.hget("exec_to_retrieve", to_retrieve_list[0]);
            std::vector<std::string> tmp_list;
            split_str(exec_str, '_', tmp_list);
            fmt::print("Retrieving: {}, {}\n", to_retrieve_list[0], tmp_list[1]); 
        }
    }

    for (int index(0); index < n_to_retrieve; index++) {
        std::string exec_uuid = to_retrieve_list[index];
        if (redis.hexists("started_time", exec_uuid) == false) {
            redis.hset("started_time", exec_uuid, fmt::format("{}", get_time()));
        }
        long added_time_l = 0;
        std::string exec_str = *redis.hget("exec_to_retrieve", exec_uuid);
        std::vector<std::string> tmp_list;
        split_str(exec_str, '_', tmp_list);

        std::string emo_uuid = tmp_list[0];
        std::string scenario_id = tmp_list[1];

        bool flag = false;

        if (is_finished(scenario_id) == true) {
            if (execute_20(scenario_id, emo_uuid, exec_uuid) == true) {
                auto cinfo = fmt::format("[Retrieved] EMO_UUID={}  EXEC_UUID={} Scenario ID={}", emo_uuid, exec_uuid, scenario_id);

                send_message(emo_uuid, exec_uuid);
                fmt::print("Retrieved: {}, {}\n", exec_uuid, scenario_id);
                redis.rpush("scenario_ids", scenario_id);
                redis.hdel("exec_to_retrieve", exec_uuid);
                redis.hdel("started_time", exec_uuid);
                continue;
            }
        }

        unsigned long long now_millisec = get_time();
        unsigned long long started_time = std::stol(*redis.hget("started_time", exec_uuid));
        unsigned long long waiting_time = 100000;
        try {
            waiting_time = std::stol(OPT4CAST_WAIT_MILLISECS_IN_CAST);
        }
        catch (const std::exception &error) {
            std::cout << "Error on retrieve_exec\n" << error.what() << "\n";
            auto cerror =  fmt::format("When trying using stoi for (OPT4CAST_WAIT_MILLISECS_IN_CAST): {} ", error.what());
        }
        std::string core_path = fmt::format("lambdarequests/optimize/optimizeSce_{}.json", exec_uuid);

        if (awss3::is_object("cast-optimization-dev", core_path) == false || 
                now_millisec - started_time > waiting_time) 
        {
            double fake_load = 9999999999999.99; //DBL_MAX; would it work?
            std::cout << fmt::format("{} - {} = {}: Waiting time: {}\n", now_millisec, started_time, now_millisec - started_time, waiting_time);

            redis.hset("executed_results", exec_uuid, fmt::format("{:.2f}_{:.2f}_{:.2f}", fake_load, fake_load, fake_load));
            send_message(emo_uuid, exec_uuid);
            //redis.rpush("executed_scenario_list", exec_uuid);//, fmt::format("{}_{}", emo_uuid, scenario_id));
            redis.rpush("scenario_ids", scenario_id);
            redis.hdel("exec_to_retrieve", exec_uuid);
            redis.hdel("started_time", exec_uuid);
            /*
            std::cout << fmt::format("{} - {} = {}\n", now_millisec, added_time_l, now_millisec - added_time_l);
            redis.rpush("retrieving_enqueue_failed", exec_uuid);
            std::cout << "retrieving enqueue failed" << exec_uuid << std::endl;
            auto cerror = fmt::format("The solution took more time than the assigned");
            */
        } else {
            redis.rpush("retrieving_queue", exec_uuid);
            //redis.rpush("added_to_retrieving_queue", fmt::format("{}", added_time_l));
        }
    }

}


//bool execute_4(int scenario_id, std::string emo_uuid) {
/*
  GUID = str(uuid.uuid4());
  json_core_content = db.exec_data[exec_data_id].json_core;
  json_core = json_core_content%(ScenarioId, GUID);
  rest = a.put_files_core_land(ScenarioId, GUID, json_core);
  */
//}

bool emo_to_initialize(std::string emo_uuid) {

    auto scenario_id = *redis.hget("emo_to_initialize", emo_uuid);
    auto [core, scenario, geography] = create_jsons(scenario_id, emo_uuid);
    auto core_str = *redis.hget(emo_uuid, "core");
    auto scenario_str = *redis.hget(emo_uuid, "scenario");
    auto geography_str = *redis.hget(emo_uuid, "geography");

    auto exec_uuid = emo_uuid;

    //std::string filename = fmt::format("{}/bmp_grp_src_vec.csv", path);
    //std::ofstream bmp_grp_src_vec_file(filename);
    //bmp_grp_src_vec_file.precision(2);

    if (send_json_streams(scenario_id, emo_uuid, exec_uuid) == true) {
        std::cout << fmt::format("emo_to_initialize emo_uuid: {} scenario_id: {}\n", emo_uuid, scenario_id);

        auto cinfo =  fmt::format("[Initialzing] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
        redis.hset("exec_to_retrieve", exec_uuid, fmt::format("{}_{}", emo_uuid, scenario_id));
        redis.rpush("retrieving_queue", exec_uuid);

        redis.hset("started_time", exec_uuid, fmt::format("{}", get_time()));
        //redis.rpush("added_to_retrieving_queue", fmt::format("{}", get_time()));
        cinfo =  fmt::format("[Retrieving QUEUE] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
        if (redis.hdel("emo_to_initialize", emo_uuid)) {

            cinfo =  fmt::format("[EMO Initialized and removed from queue] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
        } else {
            auto cerror =  fmt::format("[[DELETE EMO FROM INITALIZED FAILED] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
        }
    } else {
        redis.rpush("emo_failed_to_initialize", emo_uuid);
        std::clog << "it did not sent \n";
        auto cerror = fmt::format("[INITIALIZZATION_FAILED] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
    }
    //start_time[emo_uuid] = datetime.datetime.now()
    return true;
}

bool solution_to_execute(std::string exec_uuid) {
    std::string exec_str = *redis.hget("solution_to_execute_dict", exec_uuid);
    if (exec_str.size() <= 0) {
        std::cout << "hay pedo\n";
        exit(-1);
    }
    std::vector<std::string> exec_list;
    split_str(exec_str, '_', exec_list);

    auto emo_uuid = exec_list[0];
    auto scenario_id = exec_list[1];

    if (redis.hexists(emo_uuid, "core") == false) {
        create_jsons(scenario_id, emo_uuid);
    }

    auto core_str = *redis.hget(emo_uuid, "core");
    auto scenario_str = *redis.hget(emo_uuid, "scenario");
    auto geography_str = *redis.hget(emo_uuid, "geography");

    //emo_uuid must be deleted after emo finishes
    bool flag = true;
    if (send_files(scenario_id, emo_uuid, exec_uuid) == true) {
        if (send_json_streams(scenario_id, emo_uuid, exec_uuid, false) == true) {
            auto cinfo =  fmt::format("[Executing] EXEC_UUID: {} EMOO_UUID: {} Scenario ID: {} ", exec_uuid, emo_uuid, scenario_id);
            redis.hset("exec_to_retrieve", exec_uuid, fmt::format("{}_{}", emo_uuid, scenario_id));
            redis.rpush("retrieving_queue", exec_uuid);
            redis.hset("started_time", exec_uuid, fmt::format("{}", get_time()));
            cinfo =  fmt::format("[Retrieving QUEUE] EXEC_UUID: {} EMOO_UUID: {} Scenario ID: {}", exec_uuid, emo_uuid, scenario_id);
            //start_time[emo_uuid] = datetime.datetime.now()
            //
            redis.hdel("solution_to_execute_dict", exec_uuid);
            if (redis.hexists("solution_to_execute_dict", exec_uuid)) {
                auto cerror = fmt::format("[solution was not successfully deleted it] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
                std::cout<<cerror<<"\n";
            }
        } else {
            auto cerror =  fmt::format("[SCENARIO_SUBMISSION] EMOO_UUID: {} EXEC_UUID: {} Scenario ID: {}\n", emo_uuid, exec_uuid, scenario_id);
            flag = false;
        }
    } else {
            auto cerror =  fmt::format("[SEND_TO_EXECUTION_FAILED] EMOO_UUID: {} EXEC_UUID: {} Scenario ID: {}\n", emo_uuid, exec_uuid, scenario_id);
            std::clog << cerror;
            std::cout<<cerror<<"\n";
            flag = false;
    }
    if (flag == false) {
        std::cout<<"Returning in Solution to execute==================="<<"\n";


        double fake_load = 9999999999999.99; //DBL_MAX; would it work?

        redis.hset("executed_results", exec_uuid, fmt::format("{:.2f}_{:.2f}_{:.2f}", fake_load, fake_load, fake_load));
        send_message(emo_uuid, exec_uuid);
        redis.hdel("started_time", exec_uuid);
        if (redis.hexists("exec_to_retrieve", exec_uuid)) {
            redis.hdel("exec_to_retrieve", exec_uuid);
        }
        redis.rpush("scenario_ids", scenario_id);
    }

    return flag;
}

bool check_amqp() {
    AmqpClient::Channel::OpenOpts opts;
    opts.host = AMQP_HOST;
    opts.port = std::stoi(AMQP_PORT);
    std::string username = AMQP_USERNAME;
    std::string password = AMQP_PASSWORD;
    opts.vhost = "/";
    opts.frame_max = 131072;
    opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth(username, password);
    auto ret_value = false;
    try {
        auto channel = AmqpClient::Channel::Open(opts);
        ret_value = true;
    }
    catch (const std::exception &error) {
        auto cerror =  fmt::format("Error on sending scenarios: ", error.what());
    }
    return ret_value;
}

void send() {

    AmqpClient::Channel::OpenOpts opts;
    opts.host = AMQP_HOST;
    opts.port = std::stoi(AMQP_PORT);
    std::string username = AMQP_USERNAME;
    std::string password = AMQP_PASSWORD;
    opts.vhost = "/";
    opts.frame_max = 131072;
    opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth(username, password);
    //try {
        auto channel = AmqpClient::Channel::Open(opts);

        auto passive = false; //meaning you want the server to create the exchange if it does not already exist.
        auto durable = true; //meaning the exchange will survive a broker restart
        auto auto_delete = false; //meaning the queue will not be deleted once the channel is closed
        channel->DeclareExchange(EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, passive, durable, auto_delete);

        auto generate_queue_name ="";
        auto exclusive = false; //meaning the queue can be accessed in other channels
        auto queue_name = channel->DeclareQueue(generate_queue_name, passive, durable, exclusive, auto_delete);


        //std::clog << "Queue with name '" << queue_name << "' has been declared.\n";
        auto cinfo =  fmt::format("Queue with name {} has been declared.\n", queue_name);
        channel->BindQueue(queue_name, EXCHANGE_NAME, "opt4cast_initialization");
        channel->BindQueue(queue_name, EXCHANGE_NAME, "opt4cast_execution");

        channel->BindQueue(queue_name, EXCHANGE_NAME, "opt4cast_mathmodel_execution");
        channel->BindQueue(queue_name, EXCHANGE_NAME, "opt4cast_begin_generation");
        channel->BindQueue(queue_name, EXCHANGE_NAME, "opt4cast_end_generation");
        channel->BindQueue(queue_name, EXCHANGE_NAME, "opt4cast_begin_emo");
        channel->BindQueue(queue_name, EXCHANGE_NAME, "opt4cast_end_emo");
        channel->BindQueue(queue_name, EXCHANGE_NAME, "opt4cast_get_var_and_cnstr");

    /*

    else if (routing_key == std::string("opt4cast_mathmodel_execution")) {
    } else if (routing_key == std::string("opt4cast_begin_generation")) {
    } else if (routing_key == std::string("opt4cast_end_generation")) {
    } else if (routing_key == std::string("opt4cast_begin_emo")) {
    } else if (routing_key == std::string("opt4cast_end_emo")) {
    } else if (routing_key == std::string("opt4cast_get_var_and_cnstr")) {
    }
     */
        std::cout << " [*] Waiting for executions. To exit press CTRL+C\n";

        auto no_local = false; 
        auto no_ack = true; //meaning the server will NOT expect an acknowledgement of messages delivered to the consumer
        auto message_prefetch_count = 1;
        auto consumer_tag = channel->BasicConsume(queue_name, generate_queue_name, no_local, no_ack, exclusive, message_prefetch_count);

        //std::clog

        cinfo =  fmt::format("Consumer tag: {}\n", consumer_tag);
        while (true) {
            auto envelope = channel->BasicConsumeMessage(consumer_tag);
            auto message_payload = envelope->Message()->Body();
            auto routing_key = envelope->RoutingKey();

            if (routing_key == std::string("opt4cast_initialization")) {
                auto cinfo =  fmt::format("Initializing scenario: {}", message_payload);
                std::cout << "Initializing scenario: " << message_payload << "\n";
                emo_to_initialize(message_payload);
            } else if (routing_key == std::string("opt4cast_execution")) {
                /*
                auto cinfo =  "Sending scenario to execution queue" << message_payload;
                std::cout << "sending execution scenario: " << message_payload <<"\n";
                redis.rpush("opt4cast_execution_queue", message_payload);
                */

                auto cinfo =  fmt::format("Sending scenario to execution queue", message_payload);
                std::cout << "sending execution scenario: " << message_payload << "\n";
                //redis.rpush("opt4cast_execution_queue", message_payload);


                //std::vector<std::string> to_retrieve_list;
                int concurrent_evaluation = 10;
                int seconds = 1;
                std::vector<std::string> to_retrieve_list;
                while (to_retrieve_list.size() >= concurrent_evaluation) {
                    std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
                    to_retrieve_list.clear();
                    redis.lrange("retrieving_queue", 0, -1, std::back_inserter(to_retrieve_list));
                }
                solution_to_execute(message_payload);

            }
        }
    /*}
    catch (const std::exception &error) {
        auto cerror =  "Error on sending scenarios: " << error.what();
    }*/

}

bool retrieve() {
   //try {
        while (true) {
            retrieve_exec();


/*
std::vector<std::string> to_retrieve_list;
redis.lrange("retrieving_queue", 0, -1, std::back_inserter(to_retrieve_list));
int concurrent_evaluation = 10;
if( (int) to_retrieve_list.size() < concurrent_evaluation) {
int space = concurrent_evaluation - (int) to_retrieve_list.size();
std::vector<std::string> in_queue;
redis.lrange("opt4cast_execution_queue", 0, -1, std::back_inserter(in_queue));
int in_queue_size = in_queue.size();
for(int i(0); i< std::min(in_queue_size, space); ++i) {
std::string message = *redis.lpop("opt4cast_execution_queue");
solution_to_execute(message);
}
}
*/
            //failed initializations
            //

            std::vector<std::string> init_failed_list;

            redis.lrange("emo_failed_to_initialize", 0, -1, std::back_inserter(init_failed_list));
            for (auto emo_uuid: init_failed_list) {
                std::cout << "emo_to_initilize\n";
                emo_to_initialize(emo_uuid);
            }

            //failed send to executions

            std::vector<std::string> failed_exec_uuids;
            redis.lrange("solution_failed_to_execute_dict", 0, -1, std::back_inserter(failed_exec_uuids));
            redis.del({"solution_failed_to_execute_dict"});
            for (auto exec_uuid: failed_exec_uuids) {
                std::cout << "solution_to_execute\n";
                solution_to_execute(exec_uuid);
            }


            //sleep the thread
            int seconds = 1;
            std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
        }
    /*}
    catch (const std::exception &error) {
        auto cerror =  "Error on retrieving scenarios: " << error.what();
        exit(-1);
    }*/
    return false;
}

int main(int argc, char const *argv[]) {
    my_time = get_time();
    std::string env_var = "MSU_CBPO_PATH";
    msu_cbpo_path = get_env_var(env_var);
    std::cout << "msu_cbpo_path" << msu_cbpo_path << std::endl;

    std::string options[] = {"send", "retrieve", "log", "initialize_and_retrieve_data"};

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " [send | retrieve | log]\n";
        return -1;
    }


/* gtp: loki
  // Check if Loki is up
  if (!registry.Ready()) {
    std::cout<<"no loki up\n";
    return 1;
  }
  */

    // Create an agent with extended labels

    // Add logs to queue and wait for flush
    auto current_option = argv[1];

    Aws::SDKOptions aws_options;
    //aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Error;
    Aws::InitAPI(aws_options);
    {

        if (find(cbegin(options), cend(options), current_option) == cend(options)) {
            std::cerr << "Invalid option: " << current_option << ", please use [send | retrieve]\n";
            return -2;
        }

        if (current_option == std::string("send")) {
            auto cinfo =  "Starting send scenario deamon";
            while (!check_amqp()) {
                int seconds = 10;
                std::cout << fmt::format("Sleeping for {} seconds\n", seconds);
                sleep(seconds);
            }
            send();
        } else if (current_option == std::string("retrieve")) {
            auto cinfo =  "Starting retrieve scenario deamon";
            while (!check_amqp()) {
                int seconds = 10;
                std::cout << fmt::format("Sleeping for {} seconds\n", seconds);
                sleep(seconds);
            }
            retrieve();
        } else if (current_option == std::string("log")) {
            /*
            auto cinfo =  "Evaluate";
            std::cout<<"evaluate"<<std::endl;
              while (!check_amqp()) {
                  int seconds = 10;
                  std::cout<<fmt::format("Sleeping for {} seconds\n", seconds);
                  sleep(seconds);
              }
            log();
             */
        } else if (current_option == std::string("initalize_and_retrieve_data")) {
            std::cout << "initialize_and_retrieve_data" << std::endl;
            auto cinfo =  "Initialize and retrieve Data";
        }
    }

    ShutdownAPI(aws_options);
    return 0;
}

