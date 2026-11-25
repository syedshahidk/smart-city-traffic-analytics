import { useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup, Polyline, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';
import markerIcon from 'leaflet/dist/images/marker-icon.png';
import markerIconRetina from 'leaflet/dist/images/marker-icon-2x.png';
import markerShadow from 'leaflet/dist/images/marker-shadow.png';
import type { LatLngBoundsExpression } from 'leaflet';

import type { Coordinate, RouteAlternative } from '../types/api';

// Ensure Leaflet's default marker images resolve through Vite.
L.Icon.Default.mergeOptions({
  iconRetinaUrl: markerIconRetina,
  iconUrl: markerIcon,
  shadowUrl: markerShadow,
});

const ROUTE_COLORS = ['#22d3ee', '#a855f7', '#f59e0b', '#34d399', '#fb923c'];

function routeColor(index: number, selected: boolean) {
  const base = ROUTE_COLORS[index % ROUTE_COLORS.length];
  return selected ? base : `${base}80`; // 50% opacity for non-selected
}

// Fit the map to a bounding box once the map instance is available.
function FitBounds({ bounds }: { bounds: LatLngBoundsExpression }) {
  const map = useMap();
  useEffect(() => {
    if (bounds) {
      map.fitBounds(bounds, { padding: [70, 70], maxZoom: 15 });
    }
  }, [map, bounds]);
  return null;
}

interface RouteMapProps {
  routes: RouteAlternative[];
  source: Coordinate;
  destination: Coordinate;
  selectedRouteId: string | null;
  height?: string;
}

export function RouteMap({
  routes,
  source,
  destination,
  selectedRouteId,
  height = '520px',
}: RouteMapProps) {
  const center: [number, number] = [
    (source.lat + destination.lat) / 2,
    (source.lon + destination.lon) / 2,
  ];

  // Build a bounding box that covers source, destination, and all route geometry.
  const allCoords: [number, number][] = [
    [source.lat, source.lon],
    [destination.lat, destination.lon],
    ...routes.flatMap((r) => r.geometry),
  ];
  const lats = allCoords.map((c) => c[0]);
  const lons = allCoords.map((c) => c[1]);
  const bounds: LatLngBoundsExpression = [
    [Math.min(...lats), Math.min(...lons)],
    [Math.max(...lats), Math.max(...lons)],
  ];

  return (
    <MapContainer
      center={center}
      zoom={11}
      style={{ height, width: '100%', borderRadius: '10px', border: '1px solid #334155' }}
    >
      <TileLayer
        attribution='&copy; <a href="https://openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <FitBounds bounds={bounds} />
      <Marker position={[source.lat, source.lon]}>
        <Popup>Source</Popup>
      </Marker>
      <Marker position={[destination.lat, destination.lon]}>
        <Popup>Destination</Popup>
      </Marker>
      {routes.map((route, index) => {
        const selected = route.id === selectedRouteId;
        return (
          <Polyline
            key={route.id}
            positions={route.geometry.map(([lat, lon]) => [lat, lon])}
            color={routeColor(index, selected)}
            weight={selected ? 5 : 3}
            opacity={selected ? 1 : 0.55}
            dashArray={selected ? undefined : '7 8'}
          />
        );
      })}
    </MapContainer>
  );
}
