package com.tw.apps

case class StationData(
                          bikes_available: Integer, docks_available: Integer,
                          is_renting: Boolean, is_returning: Boolean,
                          last_updated: Long,
                          station_id: String, name: String,
                          latitude: Option[Double], longitude: Option[Double]
                        )

case class ValidatedStationData (
                                  bikes_available: Integer, docks_available: Integer,
                                  is_renting: Boolean, is_returning: Boolean,
                                  last_updated: Long,
                                  station_id: String, name: String,
                                  latitude: Option[Double], longitude: Option[Double],
                                  is_valid: Boolean
                                )
