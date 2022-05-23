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

package metrics

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type dbStatsCollector struct {
	pool *pgxpool.Pool

	eodDaily  *prometheus.Desc
	eodNoFigi *prometheus.Desc

	assetsNew     *prometheus.Desc
	assetsChanged *prometheus.Desc
	assetsRetired *prometheus.Desc
	assetsNoCusip *prometheus.Desc
	assetsNoFigi  *prometheus.Desc

	seekingAlphaDaily   *prometheus.Desc
	zacksFinancialDaily *prometheus.Desc
}

func NewDbStatsCollector(pool *pgxpool.Pool) prometheus.Collector {
	fqName := func(ns, subsystem, name string) string {
		return ns + "_" + subsystem + "_" + name
	}
	return &dbStatsCollector{
		pool: pool,

		eodDaily: prometheus.NewDesc(
			fqName("pvdb", "eod", "daily"),
			"Number of EOD quotes downloaded today",
			nil,
			nil,
		),

		eodNoFigi: prometheus.NewDesc(
			fqName("pvdb", "eod", "no_figi"),
			"Number of EOD quotes downloaded today",
			nil, nil),

		assetsNew: prometheus.NewDesc(
			fqName("pvdb", "assets", "new"),
			"Number of new assets in the last 24 hours",
			nil, nil),

		assetsChanged: prometheus.NewDesc(
			fqName("pvdb", "assets", "changed"),
			"Number of changed assets in the last 24 hours",
			nil, nil),

		assetsRetired: prometheus.NewDesc(
			fqName("pvdb", "assets", "retired"),
			"Number of retired assets in the last 24 hours",
			nil, nil),

		assetsNoCusip: prometheus.NewDesc(
			fqName("pvdb", "assets", "no_cusip"),
			"Number of assets with no CUSIP",
			nil, nil),

		assetsNoFigi: prometheus.NewDesc(
			fqName("pvdb", "assets", "no_figi"),
			"Number of assets with no Composite FIGI",
			nil, nil),

		seekingAlphaDaily: prometheus.NewDesc(
			fqName("pvdb", "seeking_alpha", "daily"),
			"Number of Seeking Alpha ratings in last 24 hours",
			nil, nil),

		zacksFinancialDaily: prometheus.NewDesc(
			fqName("pvdb", "zacks_finance", "daily"),
			"Number of Zacks Finance records in last 24 hours",
			nil, nil),
	}
}

// Describe implements Collector.
func (c *dbStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.eodDaily
	ch <- c.eodNoFigi

	ch <- c.assetsNew
	ch <- c.assetsChanged
	ch <- c.assetsRetired
	ch <- c.assetsNoCusip
	ch <- c.assetsNoFigi

	ch <- c.seekingAlphaDaily
	ch <- c.zacksFinancialDaily
}

// Collect implements Collector.
func (c *dbStatsCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.eodDaily, prometheus.GaugeValue, EodDaily(c.pool))
	ch <- prometheus.MustNewConstMetric(c.eodNoFigi, prometheus.GaugeValue, EodNoFigi(c.pool))

	ch <- prometheus.MustNewConstMetric(c.assetsNew, prometheus.GaugeValue, AssetsNew(c.pool))
	ch <- prometheus.MustNewConstMetric(c.assetsChanged, prometheus.GaugeValue, AssetsChanged(c.pool))
	ch <- prometheus.MustNewConstMetric(c.assetsRetired, prometheus.GaugeValue, AssetsRetired(c.pool))
	ch <- prometheus.MustNewConstMetric(c.assetsNoCusip, prometheus.GaugeValue, AssetsNoCUSIP(c.pool))
	ch <- prometheus.MustNewConstMetric(c.assetsNoFigi, prometheus.GaugeValue, AssetsNoFigi(c.pool))

	ch <- prometheus.MustNewConstMetric(c.seekingAlphaDaily, prometheus.GaugeValue, SeekingAlphaDaily(c.pool))
	ch <- prometheus.MustNewConstMetric(c.zacksFinancialDaily, prometheus.GaugeValue, ZacksFinanceDaily(c.pool))
}

func EodDaily(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM eod WHERE event_date::date = (now() - '1 day'::interval)::date").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve eod daily count")
		return 0
	}
	return float64(cnt)
}

func EodNoFigi(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM eod WHERE composite_figi=''").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve eod w/ no figi")
		return 0
	}
	return float64(cnt)
}

func AssetsNew(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM assets WHERE new = True").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve num new assets")
		return 0
	}
	return float64(cnt)
}

func AssetsChanged(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM assets WHERE updated = True").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve num assets changed")
		return 0
	}
	return float64(cnt)
}

func AssetsRetired(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM assets WHERE active = False AND updated = True").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve num assets retired")
		return 0
	}
	return float64(cnt)
}

func AssetsNoCUSIP(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM assets WHERE cusip = ''").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve assets w/ no cusip")
		return 0
	}
	return float64(cnt)
}

func AssetsNoFigi(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM eod WHERE event_date::date = (now() - '1 day'::interval)::date").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve assets w/ no figi")
		return 0
	}
	return float64(cnt)
}

func SeekingAlphaDaily(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM seeking_alpha WHERE event_date::date = now()::date").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve seeking alpha daily count")
		return 0
	}
	return float64(cnt)
}

func ZacksFinanceDaily(conn *pgxpool.Pool) float64 {
	var cnt int
	err := conn.QueryRow(context.Background(), "SELECT count(*) AS cnt FROM zacks_financials WHERE event_date::date = now()::date").Scan(&cnt)
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve zacks finance daily count")
		return 0
	}
	return float64(cnt)
}
