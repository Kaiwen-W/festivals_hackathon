import React, { useEffect, useState } from "react";
import { MapContainer, TileLayer, Marker, Popup, useMapEvents } from "react-leaflet";
import "react-datepicker/dist/react-datepicker.css";
import DatePicker from "react-datepicker";
import L from "leaflet";

//import logo from './logo.svg';
import './App.css';


const mapIcon = (colour) => 
  L.divIcon({
    className: "eventPoint",
    html: `<div style="
      width: 8px; height: 8px;
      border-radius: 50%;
      border:2px solid white;
      background:${colour || "green"};

      color: blue"></div>`,
    iconSize: [12,12]
  });

function fetchh(id, setBus) {
  console.log("id",id)
  let word = "http://localhost:8000/event/"+id;
  console.log(word);
  fetch(word)
    .then((res) => res.json())
    .then((data) => {
      console.log("data", data); //fetching works
      setBus(data);
  });
}

function VehicleComponent({bus}) {
  console.log("rendering buses!!!")
  console.log("Buses", bus);
  if (bus.nearby_stops) {
        return (<>
        {
          bus.nearby_stops.map((st,index) => {
            console.log("stop:", st);
            return (
              <Marker
                key={index}
                position={[st.latitude,st.longitude]}
                icon={mapIcon("red")}
              >
                <Popup><st>{st.stop_name}</st><p> is {st.distance_meters}m away</p>
                  <br></br><p>Routes include {st.bus_services}</p>
                  <br></br><p>{st.percentage_of_total}% of total capacity</p>
                  <br></br><p>{st.expected_passengers} people</p>
                  

                  
                </Popup>
              </Marker>
            )
          })
        }</>)
      }
      else {
        console.log("didnt work");
        return null;
      }
}

function PopupComponent({event, place, setBus}) {
  console.log("Popup!");
  console.log(place);
  console.log(event);
  let events = "events";
  if (event.length === 1) events="event";
  return (
    <div className="popupUnit">
      <strong>{place.name} </strong><p>{event.length} {events}</p>
      <div className="scrollUnit">
      {event.map((ev) => {
        var myDate = new Date(ev.start_ts);
        var dated = myDate.toLocaleString();
        console.log(ev);
        return(
          <div><p>{ev.name} {dated}</p> <button onClick={() => fetchh(ev.event_id, setBus)}>Details</button></div>
        )
      })}
      </div>
    </div>
  )
}

function EventsComponent({sdate, edate, setBus}) {

  const [places, setPlaces] = useState([]);
  const [events, setEvents] = useState([]);
  const [centre, setCentre] = useState([55.95, -3.18]);


  useEffect(() => {
    fetchh(1, setBus);
  }, []);

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
        name: place.name,
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

    return filtered;
  }

  function validEvent(event) {
    
    var validated = []
    //console.log(event);
    //console.log(typeof(event));
    event.forEach((ev, index) => {
      //console.log(ev);
      var start = new Date(ev.start_ts);
      //var end = new Date(ev.end_ts);

      //console.log(start, end);
      //console.log(sdate, edate);
      if (start >= sdate && start <= edate) {
        validated.push(ev);
      } //therefore valid as after start date and before end date
    });
    return validated;
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
      setEvents(dict);
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
    Object.keys(events).map(function(index, number) { //index is place id
      let event = events[index];
      //console.log("ei", event, index);
      const latd = 0.010;
      const lond = 0.030;
      //console.log("toilet");
      if (places[index] !== undefined) {
        let temp = places[index]
        //console.log("first test");
        //console.log(centre.lat, temp.lat);
        //console.log(event);
        if ((Math.abs(temp.lat-centre.lat) <= latd && Math.abs(temp.lon-centre.lng) <= lond) || true) {
          //console.log("ohio toilet");
          //console.log(event.descriptions);
          let validEvents = validEvent(event);

          //here should work on filtering etc. then return length of new set of events, and submit those to the
          //popup for rendering
          if (validEvents.length > 0) {
            return (
              <Marker
                key = {index}
                position={[temp.lat, temp.lon]}
                icon={mapIcon("blue")}
                total={validEvents.length}
              >
                <Popup>
                  <PopupComponent event={validEvents} place={temp} setBus={setBus}/>
                </Popup>
              </Marker>
            )
          }
          else return null;
        }
        else return null;
      }
      return null;
    })
  }
  </>
  )
}

function DateComponent({sdate, setsDate,edate, seteDate, bus}) {


  return (
    <div className="menu" style={{padding: "20px"}}>

      <DatePicker enableTabLoop={false} style={{zIndex: 10}} selected={sdate} onChange={(sdate) => setsDate(sdate)} />
      <DatePicker enableTabLoop={false} style={{zIndex: 10}} selected={edate} onChange={(edate) => seteDate(edate)} />

    </div>
  );
}

export default function App() {
  const [sdate, setsDate] = useState(new Date());
  const [edate, seteDate] = useState(new Date());
  const [bus, setBus] = useState([]);


  return (
    <>
    
      <DateComponent
      sdate={sdate} 
      setsDate={setsDate}
      edate={edate}
      seteDate={seteDate}
      bus={bus}
      style={{minHeight: "40px", height: "40px", margin: "2px"}}
        
      />
      <div style={{width: "100%", height: "100%", margin: "auto"}}>
        <MapContainer
          center={[55.89, -3.72]}
          zoom={10}
          style={{width: "100%", zIndex: "0", height: "100%"}}
        >
          <TileLayer
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <VehicleComponent bus={bus}/>
          <EventsComponent 
            sdate={sdate}
            edate={edate}
            setBus={setBus}
          />

        </MapContainer>
      </div>
    </>
  );
}
