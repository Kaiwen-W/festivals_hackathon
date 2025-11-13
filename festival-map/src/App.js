import React, { useEffect, useState } from "react";
import { MapContainer, TileLayer, Marker, Popup, useMapEvents } from "react-leaflet";
import L from "leaflet";
//import logo from './logo.svg';
import './App.css';


const mapIcon = () => 
  L.divIcon({
    className: "eventPoint",
    html: `<div style="
      width: 8px; height: 8px;
      border-radius: 50%;
      border:2px solid white;
      background: blue"></div>`,
    iconSize: [8,8]
  });



function EventsComponent() {

  const [places, setPlaces] = useState([]);
  const [events, setEvents] = useState([]);
  const [centre, setCentre] = useState([55.95, -3.18]);
  //const [zoom, setZoom] = useState(12);
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

  /*useEffect(() => {
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

      const info = data.events;
      //console.log(info);
      setEvents(info);
    })
  },[]);*/


  function filterr(event, place) {
    const filtered = {
      event_id: event.event_id,
      name: event.name,
      status: event.status,
      start_ts: place.start_ts,
      end_ts: place.end_ts,
      performances: place.performances,
      place: place.place
    }

    return filtered
  }

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
      //console.log(data);

      const info = data.events;
      //console.log(info);
      var dict = {};
      info.forEach((el, ind) => {

        el.schedules.forEach((place, index) => {
          if (dict[place.place_id]) {
            dict[place.place_id].push(filterr(el, place));
          }
          else {
            dict[place.place_id] = [filterr(el, place)];
          }
        })

      })
      console.log("dict below");
      console.log(dict);
      setEvents(info);
    })
  },[])

  useMapEvents({
    zoomend: (e) => {
      //setZoom(e.target.getZoom());
      setCentre(e.target.getCenter());
    },
    moveend: (e) => {
      setCentre(e.target.getCenter());
    },
  });


  return (<>
    {
    events.map((event) => {
      const latd = 0.010;
      const lond = 0.030;
      //console.log("toilet");
      if (places[event.schedules[0].place_id] !== undefined) {
        let temp = places[event.schedules[0].place_id]
        //console.log("first test");
        //console.log(centre.lat, centre.lng);
        if (Math.abs(temp.lat-centre.lat) <= latd && Math.abs(temp.lon-centre.lng) <= lond) {
        
          //console.log(event.descriptions);
          return (
            <Marker
              key = {event.event_id}
              name={event.name}
              position={[temp.lat , temp.lon]}
              icon={mapIcon()}
            >
              <Popup>
                <strong>{event.name}</strong>
                <p>{event.descriptions!== undefined ? event.descriptions[0].description : "no description"}</p>
              </Popup>
            </Marker>
          )
        }
        else return null;
      }
      return null;
    })
  }
  </>
  )
}


export default function App() {


  

  return (
    <MapContainer
      center={[55.89, -3.72]}
      zoom={10}
      style={{width: "100%", height: "100vh"}}
    >
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />

      <EventsComponent />

    </MapContainer>
  );
}
