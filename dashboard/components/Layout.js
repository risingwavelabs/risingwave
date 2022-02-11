import Head from 'next/head'
import Link from 'next/link';

import React from 'react';
import { useState } from 'react';
import { styled, useTheme } from '@mui/material/styles';
import Box from '@mui/material/Box';
import Drawer from '@mui/material/Drawer';
import CssBaseline from '@mui/material/CssBaseline';
import MuiAppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import List from '@mui/material/List';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';

import MenuIcon from '@mui/icons-material/Menu';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import DoubleArrowIcon from '@mui/icons-material/DoubleArrow';
import ViewComfyIcon from '@mui/icons-material/ViewComfy';
import InfoIcon from '@mui/icons-material/Info';

import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';

import { capitalize } from '../lib/str';

const drawerWidth = 215;

const Main = styled('main', { shouldForwardProp: (prop) => prop !== 'open' })(
  ({ theme, open }) => ({
    flexGrow: 1,
    padding: theme.spacing(3),
    transition: theme.transitions.create('margin', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
    marginLeft: `-${drawerWidth}px`,
    ...(open && {
      transition: theme.transitions.create('margin', {
        easing: theme.transitions.easing.easeOut,
        duration: theme.transitions.duration.enteringScreen,
      }),
      marginLeft: 0,
    }),
  }),
);

const AppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== 'open',
})(({ theme, open }) => ({
  transition: theme.transitions.create(['margin', 'width'], {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
  ...(open && {
    width: `calc(100% - ${drawerWidth}px)`,
    marginLeft: `${drawerWidth}px`,
    transition: theme.transitions.create(['margin', 'width'], {
      easing: theme.transitions.easing.easeOut,
      duration: theme.transitions.duration.enteringScreen,
    }),
  }),
}));

const DrawerHeader = styled('div')(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  padding: theme.spacing(0, 1),
  // necessary for content to be below app bar
  ...theme.mixins.toolbar,
  justifyContent: 'space-between',
}));

const NavBarNavigationItem = styled('div')(() => ({
  width: "100%",
  display: "flex",
  flexDirection: "row",
  alignItems: "center"
}));

const NavBarItem = (props) => {
  return (
    <ListItemButton key={props.text} selected={props.currentPage === props.text}>
      <Link href={"/" + props.text}>
        <NavBarNavigationItem>
          <ListItemIcon>
            {props.icon}
          </ListItemIcon>
          <span style={{ fontSize: "15px" }}>{capitalize(props.text)}</span>
        </NavBarNavigationItem>
      </Link>
    </ListItemButton>
  )
}

export default function Home(props) {
  const theme = useTheme();
  const [open, setOpen] = useState(true);
  const [currentPage, setCurrentPage] = useState(props.currentPage ? props.currentPage : " ");

  const handleDrawerOpen = () => {
    setOpen(true);
  };

  const handleDrawerClose = () => {
    setOpen(false);
  };

  const WrapNavItem = (props) => (
    <>
      <NavBarItem
        text={props.text}
        icon={props.icon}
        currentPage={currentPage}
        setCurrentPage={setCurrentPage}
      />
    </>
  )

  return (
    <>
      <Head>
        <title>Dashboard | RisingWave</title>
        <link rel="icon" href="/singularitydata.svg" />
      </Head>
      <Box sx={{ display: 'flex' }}>
        <CssBaseline />
        <AppBar open={open}>
          <Toolbar>
            <IconButton
              color="inherit"
              aria-label="open drawer"
              onClick={handleDrawerOpen}
              edge="start"
              sx={{ mr: 2, ...(open && { display: 'none' }) }}
            >
              <MenuIcon />
            </IconButton>
            <div>
              {capitalize(currentPage)}
            </div>
          </Toolbar>
        </AppBar>
        <Drawer
          sx={{
            width: drawerWidth,
            flexShrink: 0,
            '& .MuiDrawer-paper': {
              width: drawerWidth,
              boxSizing: 'border-box',
            },
          }}
          variant="persistent"
          anchor="left"
          open={open}
        >
          <DrawerHeader>

            <div style={{ display: "flex", flexDirection: "column", marginLeft: "5px" }}>
              <div style={{ display: "flex", flexDirection: "row", alignItems: "center" }}>
                <img src="/singularitydata.svg" width="20px" height="20px" />
                <span style={{ fontSize: "15px", fontWeight: "700", marginLeft: "5px" }}>RisingWave</span>
              </div>
              <div>
                <span style={{ fontSize: "13px" }}>Dashboard </span>
                <span style={{ fontSize: "13px" }}>v0.0.1-alpha</span>
              </div>
            </div>
            <IconButton onClick={handleDrawerClose}>
              {theme.direction === 'ltr' ? <ChevronLeftIcon /> : <ChevronRightIcon />}
            </IconButton>
          </DrawerHeader>
          <Divider />
          <List>
            <WrapNavItem
              text='cluster'
              icon={<ViewComfyIcon fontSize="small" />}
            />
            <WrapNavItem
              text='streaming'
              icon={<DoubleArrowIcon fontSize="small" />}
            />
          </List>
          <Divider />
          <List>
            <WrapNavItem
              text='about'
              icon={<InfoIcon fontSize="small" />}
            />
          </List>
        </Drawer>
        <Main open={open}>
          <DrawerHeader />
          {props.children}
        </Main>
      </Box>
    </>
  );
}