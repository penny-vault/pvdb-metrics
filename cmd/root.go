// Copyright 2022
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/penny-vault/pvdb-metrics/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

type CronLog struct {
}

func keysAndValuesToEvent(event *zerolog.Event, k []interface{}) *zerolog.Event {
	// convert to map
	currKey := ""
	for idx, val := range k {
		if (idx % 2) == 0 {
			if myKey, ok := val.(string); ok {
				currKey = myKey
			} else {
				currKey = ""
			}
		} else {
			if currKey != "" {
				event.Interface(currKey, val)
			}
		}
	}

	return event
}

func (c CronLog) Info(msg string, keysAndValues ...interface{}) {
	keysAndValuesToEvent(log.Info(), keysAndValues).Msg(msg)
}

func (c CronLog) Error(err error, msg string, keysAndValues ...interface{}) {
	keysAndValuesToEvent(log.Error(), keysAndValues).Err(err).Msg(msg)
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "pvdb-metrics",
	Short: "run prometheus metrics collection for pvdb",
	Run: func(cmd *cobra.Command, args []string) {
		reg := prometheus.NewRegistry()

		pool, err := pgxpool.Connect(context.Background(), viper.GetString("database.url"))
		if err != nil {
			log.Error().Err(err).Msg("failed to connect to database via pgxpool")
		}
		defer pool.Close()

		reg.MustRegister(metrics.NewDbStatsCollector(pool))

		port := fmt.Sprintf(":%d", viper.GetInt("server.port"))
		log.Info().Int("Port", viper.GetInt("server.port")).Msg("Starting HTTP server")

		http.Handle("/metrics", promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				// Opt into OpenMetrics to support exemplars.
				EnableOpenMetrics: true,
			},
		))
		http.ListenAndServe(port, nil)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	cobra.OnInitialize(initLog)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.pvdb-metrics.yaml)")
	rootCmd.PersistentFlags().Bool("log.json", false, "print logs as json to stderr")
	viper.BindPFlag("log.json", rootCmd.PersistentFlags().Lookup("log.json"))
	rootCmd.PersistentFlags().StringP("database-url", "d", "host=localhost port=5432", "DSN for database connection")
	viper.BindPFlag("database.url", rootCmd.PersistentFlags().Lookup("database-url"))

	rootCmd.PersistentFlags().Int("port", 2112, "default port to run server on")
	viper.BindPFlag("server.port", rootCmd.PersistentFlags().Lookup("port"))
}

func initLog() {
	if !viper.GetBool("log.json") {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name "pvdb-metrics" (without extension).
		viper.AddConfigPath("/etc/") // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.config", home))
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
		viper.SetConfigName("pvdb-metrics")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debug().Str("ConfigFile", viper.ConfigFileUsed()).Msg("Loaded config file")
	} else {
		log.Error().Err(err).Msg("error reading config file")
	}
}
