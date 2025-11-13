import React, { useEffect, useState } from "react";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
//import L from "leaflet";
//import logo from './logo.svg';
import './App.css';

export default function App() {
  const [places, setPlaces] = useState([]);
  const [events, setEvents] = useState([]);

  useEffect(() => { //places data
    fetch('thistle_data.json',{
      headers : { 
        'Content-Type': 'application/json',
        'Accept': 'application/json'
       }
    })
    .then((res) => res.json())
    .then((data) => {
      console.log("Fetched data");
      //console.log(data);

      const info = data.places.map((place) => ({
        address: place.address,
        id: place.place_id,
        lat: place.loc.latitude,
        lon: place.loc.longitude,
        tags: place.tags,
        town: place.town
      }));
      //console.log(info);

      let dict = {};
      info.forEach((el, index) => dict[el.id] = el);
      console.log(dict);
      setPlaces(dict);
    })
  },[]);

  useEffect(() => {
    fetch('thistle_data.json',{
      headers : { 
        'Content-Type': 'application/json',
        'Accept': 'application/json'
       }
    })
    .then((res) => res.json())
    .then((data) => {
      console.log("Fetched data");
      console.log(data);

      const info = data.events;
      console.log(info);
      setEvents(info);
    })
  },[]);

  return (
    <MapContainer
      center={[55.89, -3.72]}
      zoom={10}
      style={{width: "100%", height: "100vh"}}
    >
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />

      {/*{
        places.map((place) => {
          if (true) {
            return (
              <Marker
                key={place.address} 
                position={[place.lat,place.lon]}
              ></Marker>
            )
          }
          return null;
        })
      }*/}

      {
        events.map((event) => {
          console.log("toilet");
          if (places[event.schedules[0].place_id] !== undefined) {
            console.log(places[event.schedules[0].place_id]);
            console.log(places[event.schedules[0].place_id].lat);
            let temp = places[event.schedules[0].place_id]
            console.log(temp.lat)
            console.log(places);
            return (
              <Marker
                key={event.name}
                position={[temp.lat , temp.lon]}
              >
                <Popup>
                  <strong>{event.name}</strong>
                  <p>{event.name}</p>
                </Popup>
              </Marker>
            )
          }
          return null;
        })
      }
    </MapContainer>
  );
}
