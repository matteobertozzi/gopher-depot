/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geo

import (
	"math"
)

const EarthRadiusKm = 6371.0710

func rad2degr(rad float64) float64 {
	return rad * 180 / math.Pi
}

func deg2rad(deg float64) float64 {
	return deg * math.Pi / 180
}

func CoordCenter(coords []Coord) Coord {
	if len(coords) == 0 {
		return Coord{}
	}
	if len(coords) == 1 {
		return coords[0]
	}

	var sumX, sumY, sumZ float64

	for _, coord := range coords {
		lat := deg2rad(coord.Lat)
		lng := deg2rad(coord.Lng)
		cosLat := math.Cos(lat)

		sumX += cosLat * math.Cos(lng)
		sumY += cosLat * math.Sin(lng)
		sumZ += math.Sin(lat)
	}

	numCoords := float64(len(coords))
	avgX := sumX / numCoords
	avgY := sumY / numCoords
	avgZ := sumZ / numCoords

	// Convert average Cartesian coordinates back to latitude and longitude
	lng := math.Atan2(avgY, avgX)
	hyp := math.Sqrt(avgX*avgX + avgY*avgY)
	lat := math.Atan2(avgZ, hyp)

	return Coord{
		Lat: rad2degr(lat),
		Lng: rad2degr(lng),
	}
}

func HaversineDistanceInKm(a, b Coord) float64 {
	rlat1 := deg2rad(a.Lat)
	rlat2 := deg2rad(b.Lat)
	diffLat := rlat2 - rlat1
	diffLon := deg2rad(b.Lng - a.Lng)

	sinDiffLat := math.Sin(diffLat / 2)
	sinDiffLon := math.Sin(diffLon / 2)

	return 2 * EarthRadiusKm * math.Asin(math.Sqrt(sinDiffLat*sinDiffLat+math.Cos(rlat1)*math.Cos(rlat2)*sinDiffLon*sinDiffLon))
}

func HaversineDistanceInMeters(a, b Coord) float64 {
	return HaversineDistanceInKm(a, b) * 1000
}

func CoordsRadiusInKm(center Coord, coords []Coord) float64 {
	var maxDistance float64
	for _, coord := range coords {
		distance := HaversineDistanceInKm(center, coord)
		if distance > maxDistance {
			maxDistance = distance
		}
	}
	return maxDistance
}

func CoordsRadiusInMeters(center Coord, coords []Coord) float64 {
	return CoordsRadiusInKm(center, coords) * 1000
}
